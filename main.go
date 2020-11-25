package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
)

func main() {
	default_parameter_groups := map[string]bool{
		"default.mysql5.7": true,
		"default.mysql5.6": true,
	}

	db_instance := result.DBInstances[0]
	found_group := *db_instance.DBParameterGroups[0].DBParameterGroupName
	engine_version := *db_instance.EngineVersion
	family := *db_instance.Engine + engine_version[0:3]
	fmt.Println(family)
	// fmt.Println(result.DBInstances[0])
	if default_parameter_groups[found_group] {
		fmt.Println("using a default group", found_group)
	}
}

func establish_session(profile, region string) *rds.RDS {
	sess, _ := session.NewSessionWithOptions(session.Options{
		// Specify profile to load for the session's config
		Profile: profile,

		// Provide SDK Config options, such as Region.
		Config: aws.Config{
			Region: aws.String(region), //parameratize this
		},
	})
	svc := rds.New(sess)
	return svc
}

func check_parameter_group(sess *rds.RDS, db_identifier string) *rds.DescribeDBInstancesOutput {
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
	return result
}

func create_parameter_group(sess *rds.RDS) {

}

func toggle_slowquery_log(sess *rds.RDS, parameter_group_name string) {

}

func download_slowquery_log() {

}

func run_query_digest() {

}
