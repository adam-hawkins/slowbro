package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
)

type DBInstance struct {
	name                 string
	engine               string
	engineVersion        string
	parameterGroupName   string
	parameterGroupFamily string
}

type TuneForm struct {
	Instance    string `json:"identifier"`
	Profile     string `json:"profile"`
	Region      string `json:"region"`
	TimerLength uint8  `json:"sampleTime"`
}

func main() {
	http.HandleFunc("/", formHandler)
	http.ListenAndServe(":8080", nil)
}

func formHandler(w http.ResponseWriter, r *http.Request) {
	defaultParameterGroups := map[string]bool{
		"default.mysql5.7": true,
		"default.mysql5.6": true,
		"default.mysql8.0": true,
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "http://localhost:3000")
	if r.Method == "POST" {
		w.WriteHeader(http.StatusOK)
		defer r.Body.Close()
		body, _ := ioutil.ReadAll(r.Body)

		form := new(TuneForm)
		json.Unmarshal(body, &form)
		sess := establishSession(form.Profile, form.Region)
		instance := checkDBInstance(sess, form.Instance)
		if defaultParameterGroups[instance.parameterGroupName] {
			createParameterGroup(sess, instance)
			time.Sleep(6000) //TODO: add better checking, don't just wait a long time
			attachParameterGroup(sess, instance.name)
		}
		toggleSlowQueryLog(sess, instance.parameterGroupName, "true")
		filename := downloadSlowQueryLog(sess, instance.name)
		downloadQueryDigest()
		runQueryDigest(filename, instance.name)

	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "Forbidden\n")
	}

}

func establishSession(profile, region string) *rds.RDS {
	sess, err := session.NewSessionWithOptions(session.Options{
		// Specify profile to load for the session's config
		Profile: profile,

		// Provide SDK Config options, such as Region.
		Config: aws.Config{
			Region: aws.String(region),
		},
	})
	if err != nil {
		fmt.Println("Error establishing session")
		panic(err.Error())
	}
	svc := rds.New(sess)
	return svc
}

func checkDBInstance(sess *rds.RDS, db_identifier string) *DBInstance {
	input := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(db_identifier), //parameratize this
	}

	result, err := sess.DescribeDBInstances(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case rds.ErrCodeDBInstanceNotFoundFault:
				fmt.Println(rds.ErrCodeDBInstanceNotFoundFault, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
	}
	foundInstance := result.DBInstances[0]
	dbInstance := DBInstance{
		name:                 *foundInstance.DBInstanceIdentifier,
		engine:               *foundInstance.Engine,
		engineVersion:        *foundInstance.EngineVersion,
		parameterGroupName:   *foundInstance.DBParameterGroups[0].DBParameterGroupName,
		parameterGroupFamily: *foundInstance.Engine + (*foundInstance.EngineVersion)[0:3],
	}
	return &dbInstance
}

func createParameterGroup(sess *rds.RDS, dbInstance *DBInstance) error {
	input := &rds.CreateDBParameterGroupInput{
		DBParameterGroupFamily: aws.String(dbInstance.parameterGroupFamily),
		DBParameterGroupName:   aws.String("slowbro-slowquery"),
		Description:            aws.String("Keep most defaults, but enable the slow query log"),
	}

	_, err := sess.CreateDBParameterGroup(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case rds.ErrCodeDBParameterGroupQuotaExceededFault:
				fmt.Println(rds.ErrCodeDBParameterGroupQuotaExceededFault, aerr.Error())
			case rds.ErrCodeDBParameterGroupAlreadyExistsFault:
				fmt.Println(rds.ErrCodeDBParameterGroupAlreadyExistsFault, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return err
	}
	dbInstance.parameterGroupName = "slowbro-slowquery"
	return nil
}

func attachParameterGroup(sess *rds.RDS, instanceName string) {
	input := &rds.ModifyDBInstanceInput{
		DBInstanceIdentifier: aws.String(instanceName),
		DBParameterGroupName: aws.String("slowbro-slowquery"),
	}

	_, err := sess.ModifyDBInstance(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case rds.ErrCodeInvalidDBInstanceStateFault:
				fmt.Println(rds.ErrCodeInvalidDBInstanceStateFault, aerr.Error())
			case rds.ErrCodeInvalidDBSecurityGroupStateFault:
				fmt.Println(rds.ErrCodeInvalidDBSecurityGroupStateFault, aerr.Error())
			case rds.ErrCodeDBInstanceAlreadyExistsFault:
				fmt.Println(rds.ErrCodeDBInstanceAlreadyExistsFault, aerr.Error())
			case rds.ErrCodeDBInstanceNotFoundFault:
				fmt.Println(rds.ErrCodeDBInstanceNotFoundFault, aerr.Error())
			case rds.ErrCodeDBSecurityGroupNotFoundFault:
				fmt.Println(rds.ErrCodeDBSecurityGroupNotFoundFault, aerr.Error())
			case rds.ErrCodeDBParameterGroupNotFoundFault:
				fmt.Println(rds.ErrCodeDBParameterGroupNotFoundFault, aerr.Error())
			case rds.ErrCodeInsufficientDBInstanceCapacityFault:
				fmt.Println(rds.ErrCodeInsufficientDBInstanceCapacityFault, aerr.Error())
			case rds.ErrCodeStorageQuotaExceededFault:
				fmt.Println(rds.ErrCodeStorageQuotaExceededFault, aerr.Error())
			case rds.ErrCodeInvalidVPCNetworkStateFault:
				fmt.Println(rds.ErrCodeInvalidVPCNetworkStateFault, aerr.Error())
			case rds.ErrCodeProvisionedIopsNotAvailableInAZFault:
				fmt.Println(rds.ErrCodeProvisionedIopsNotAvailableInAZFault, aerr.Error())
			case rds.ErrCodeOptionGroupNotFoundFault:
				fmt.Println(rds.ErrCodeOptionGroupNotFoundFault, aerr.Error())
			case rds.ErrCodeDBUpgradeDependencyFailureFault:
				fmt.Println(rds.ErrCodeDBUpgradeDependencyFailureFault, aerr.Error())
			case rds.ErrCodeStorageTypeNotSupportedFault:
				fmt.Println(rds.ErrCodeStorageTypeNotSupportedFault, aerr.Error())
			case rds.ErrCodeAuthorizationNotFoundFault:
				fmt.Println(rds.ErrCodeAuthorizationNotFoundFault, aerr.Error())
			case rds.ErrCodeCertificateNotFoundFault:
				fmt.Println(rds.ErrCodeCertificateNotFoundFault, aerr.Error())
			case rds.ErrCodeDomainNotFoundFault:
				fmt.Println(rds.ErrCodeDomainNotFoundFault, aerr.Error())
			case rds.ErrCodeBackupPolicyNotFoundFault:
				fmt.Println(rds.ErrCodeBackupPolicyNotFoundFault, aerr.Error())
			case rds.ErrCodeKMSKeyNotAccessibleFault:
				fmt.Println(rds.ErrCodeKMSKeyNotAccessibleFault, aerr.Error())
			case rds.ErrCodeInvalidDBClusterStateFault:
				fmt.Println(rds.ErrCodeInvalidDBClusterStateFault, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}
}

func toggleSlowQueryLog(sess *rds.RDS, parameterGroup, parameterValue string) error {
	modifyInput := &rds.ModifyDBParameterGroupInput{
		DBParameterGroupName: aws.String(parameterGroup),
		Parameters: []*rds.Parameter{
			{
				ApplyMethod:    aws.String("immediate"),
				ParameterName:  aws.String("slow_query_log"),
				ParameterValue: aws.String(parameterValue),
			},
			{
				ApplyMethod:    aws.String("immediate"),
				ParameterName:  aws.String("long_query_time"),
				ParameterValue: aws.String("0"),
			},
		},
	}

	_, err := sess.ModifyDBParameterGroup(modifyInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case rds.ErrCodeDBParameterGroupNotFoundFault:
				fmt.Println(rds.ErrCodeDBParameterGroupNotFoundFault, aerr.Error())
			case rds.ErrCodeInvalidDBParameterGroupStateFault:
				fmt.Println(rds.ErrCodeInvalidDBParameterGroupStateFault, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return err
	}
	return nil
}

func downloadSlowQueryLog(sess *rds.RDS, instanceName string) string {
	input := &rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: aws.String(instanceName),
		LogFileName:          aws.String("slowquery/mysql-slowquery.log"),
	}

	result, err := sess.DownloadDBLogFilePortion(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case rds.ErrCodeDBInstanceNotFoundFault:
				fmt.Println(rds.ErrCodeDBInstanceNotFoundFault, aerr.Error())
			case rds.ErrCodeDBLogFileNotFoundFault:
				fmt.Println(rds.ErrCodeDBLogFileNotFoundFault, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
	}

	filename := instanceName + "-slowquery-" + time.Now().String()
	f, err := os.Create(filename)
	if err != nil {
		fmt.Println(err)
		f.Close()
	}

	f.WriteString(*result.LogFileData)
	os.Chmod("./pt-query-digest", 777)
	return filename
}

func downloadQueryDigest() {
	f, _ := os.Create("pt-query-digest")
	resp, _ := http.Get("https://www.percona.com/get/pt-query-digest")
	_, _ = io.Copy(f, resp.Body)
	os.Chmod("pt-query-digest", 777)
}
func runQueryDigest(filename, instanceName string) {
	binary, lookErr := exec.LookPath("perl")
	if lookErr != nil {
		panic(lookErr)
	}
	runDigest := exec.Command(binary, "pt-query-digest", "<", filename)
	slowLogBytes, _ := runDigest.Output()
	slowLog := string(slowLogBytes)
	outputFilename := instanceName + "-" + time.Now().String() + "-digested"
	outputFile, _ := os.Create(outputFilename)
	outputFile.WriteString(slowLog)
}
