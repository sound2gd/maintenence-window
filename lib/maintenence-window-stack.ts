import { Duration, Stack, StackProps } from 'aws-cdk-lib';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import * as iam from "aws-cdk-lib/aws-iam";
import { Construct } from 'constructs';

export class MaintenenceWindowStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // define how frequent the workflow would be triggerred
    const repeatBySeconds = 60 * 60 * 24;
    // define when the workflow would be stopped
    const targetedDatetime = "2023-10-01T00:00:00.000Z";

    // sns topic for send out email notification
    const topic = new sns.Topic(this, 'MaintenenceWindowTopic');
    topic.addSubscription(new subs.EmailSubscription("criswang+rdsmaintenance@amazon.com"));

    // lambda for perform operations
    const maintenanceWindowAdjust = new lambda.Function(this, 'MaintenanceWindowAdjust', {
      code: lambda.Code.fromAsset('lambda-maintenance-window'),
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: "lambda_function.lambda_handler",
      timeout: Duration.seconds(900)
    });
    maintenanceWindowAdjust.role?.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonRDSFullAccess'));

    const callMaintenanceWindowAdjust = new tasks.LambdaInvoke(this, 'Perform Maintenance Window Adjustment', {
      lambdaFunction: maintenanceWindowAdjust,
      resultPath: '$.result'
    });

    // lambda for send sns
    const sendEmailNotification = new lambda.Function(this, 'SendEmailNotification', {
      code: lambda.Code.fromAsset('lambda-send-notification'),
      runtime: lambda.Runtime.PYTHON_3_9,
      environment: {
        "Topic_Arn": topic.topicArn,
      },
      handler: "lambda_function.lambda_handler",
      timeout: Duration.seconds(300)
    });
    sendEmailNotification.role?.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSNSFullAccess'));

    const callSendEmailNotification = new tasks.LambdaInvoke(this, 'Send Email Notification', {
      lambdaFunction: sendEmailNotification,
      resultPath: '$.email'
    });
    
    // step function definition
    const waitTime = Duration.seconds(repeatBySeconds);
    const repeatX = new sfn.Wait(this, 'Repeat X Seconds', {
      time: sfn.WaitTime.duration(waitTime)
    });

    const repeatTask = repeatX.next(callMaintenanceWindowAdjust)

    const definition = callMaintenanceWindowAdjust
    .next(callSendEmailNotification)
    .next(new sfn.Choice(this, 'Still within targeted date?')
    .when(sfn.Condition.timestampLessThan('$.result.Payload.CurrentTime', targetedDatetime), repeatTask)
    .otherwise(new sfn.Succeed(this, "Success")));

    const stateMachine = new sfn.StateMachine(this, 'RegularMaintenanceCheck', {
      definition
    });
  }
}
