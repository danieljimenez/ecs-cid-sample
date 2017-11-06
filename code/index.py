from __future__ import print_function

import base64
import datetime
import json
import logging

import boto3

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Establish boto3 session
session = boto3.session.Session()
logger.debug('Session is in region %s ', session.region_name)

ec2Client = session.client(service_name='ec2')
ecsClient = session.client(service_name='ecs')
asgClient = session.client(service_name='autoscaling')
snsClient = session.client(service_name='sns')
lambdaClient = session.client(service_name='lambda')


def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type while converting to JSON.")


def publish_to_sns(message, topic_arn):
    """
    Publish SNS message to trigger lambda again.

    :param message: To re-post the complete original message
        received when ASG terminating event was received.
    :param topic_arn: SNS topic to publish the message to.
    """
    logger.info("Publish to SNS topic %s", topic_arn)
    snsClient.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message, default=datetime_handler),
        Subject='Publishing SNS message to invoke lambda again..'
    )
    return "published"


def check_container_instance_task_status(ec2_instance_id):
    """
    Check task status on the ECS container instance ID.
    :param ec2_instance_id: The EC2 instance ID is used to identify the cluster, container instances in cluster
    """
    container_instance_id = None
    cluster_name = None
    tmp_msg_append = None

    # Describe instance attributes and get the cluster name from userdata section which would have set ECS_CLUSTER name
    ec2_resp = ec2Client.describe_instance_attribute(InstanceId=ec2_instance_id, Attribute='userData')
    userdata_encoded = ec2_resp['UserData']
    userdata_decoded = base64.b64decode(userdata_encoded['Value'])
    logger.debug("describe_instance_attribute response %s", ec2_resp)

    tmp_list = userdata_decoded.split()
    for token in tmp_list:
        if token.find("ECS_CLUSTER") > -1:
            # Split and get the cluster name
            cluster_name = token.split('=')[1].replace('"', '')
            logger.info("Cluster name %s", cluster_name)

    # Get list of container instance IDs from the clusterName
    paginator = ecsClient.get_paginator('list_container_instances')
    cluster_list_pages = paginator.paginate(cluster=cluster_name)
    for container_list_resp in cluster_list_pages:
        container_det_resp = ecsClient.describe_container_instances(
            cluster=cluster_name,
            containerInstances=container_list_resp['containerInstanceArns']
        )
        logger.debug(
            "describe_container_instances response: %s",
            json.dumps(container_det_resp, default=datetime_handler)
        )

        for container_instances in container_det_resp['containerInstances']:
            logger.debug(
                "Container Instance ARN: %s and EC2 Instance ID %s",
                container_instances['containerInstanceArn'],
                container_instances['ec2InstanceId']
            )
            if container_instances['ec2InstanceId'] == ec2_instance_id:
                logger.info("Container instance ID of interest : %s", container_instances['containerInstanceArn'])
                container_instance_id = container_instances['containerInstanceArn']

                # Check if the instance state is set to DRAINING.
                # If not, set it, so the ECS Cluster will handle de-registering instance,
                # draining tasks and draining them.
                container_status = container_instances['status']
                if container_status == 'DRAINING':
                    logger.info(
                        "Container ID %s with EC2 instance-id %s is draining tasks",
                        container_instance_id,
                        ec2_instance_id
                    )
                    tmp_msg_append = {"containerInstanceId": container_instance_id}
                else:
                    # Make ECS API call to set the container status to DRAINING
                    logger.info("Make ECS API call to set the container status to DRAINING...")
                    ecsClient.update_container_instances_state(
                        cluster=cluster_name,
                        containerInstances=[container_instance_id],
                        status='DRAINING'
                    )
                    # When you set instance state to draining, append the containerInstanceID to the message as well
                    tmp_msg_append = {"containerInstanceId": container_instance_id}
                break
            if container_instance_id is not None:
                break

    # Using container Instance ID, get the task list, and task running on that instance.
    if container_instance_id is not None:
        # List tasks on the container instance ID, to get task Arns
        list_task_resp = ecsClient.list_tasks(cluster=cluster_name, containerInstance=container_instance_id)
        logger.debug("Container instance task list: %s", list_task_resp['taskArns'])

        # If the chosen instance has tasks
        if len(list_task_resp['taskArns']) > 0:
            logger.info("Tasks are on this instance: %s", ec2_instance_id)
            return True, tmp_msg_append
        else:
            logger.info("NO tasks are on this instance: %s", ec2_instance_id)
            return False, tmp_msg_append
    else:
        logger.info("NO tasks are on this instance: %s", ec2_instance_id)
        return False, tmp_msg_append


def lambda_handler(event, _):
    """
    Handler called by Lambda

    :param event:
    :param _: context, unused.
    """
    logger.info("Lambda received the event %s", json.dumps(event, default=datetime_handler))

    line = event['Records'][0]['Sns']['Message']
    message = json.loads(line)

    if 'EC2InstanceId' in message:
        ec2_instance_id = message['EC2InstanceId']
        asg_group_name = message['AutoScalingGroupName']
        topic_arn = event['Records'][0]['Sns']['TopicArn']

        cluster_name = None

        # Describe instance attributes and get the cluster name from
        # userdata section which would have set ECS_CLUSTER name
        ec2_resp = ec2Client.describe_instance_attribute(InstanceId=ec2_instance_id, Attribute='userData')
        logger.debug("describe_instance_attribute response: %s", json.dumps(ec2_resp, default=datetime_handler))
        userdata_encoded = ec2_resp['UserData']
        userdata_decoded = base64.b64decode(userdata_encoded['Value'])

        tmp_list = userdata_decoded.split()
        for token in tmp_list:
            if token.find("ECS_CLUSTER") > -1:
                # Split and get the cluster name
                cluster_name = token.split('=')[1].replace('"', '')
                logger.debug("Cluster name is %s", cluster_name)

        # Get list of container instance IDs from the clusterName
        cluster_list_resp = ecsClient.list_container_instances(cluster=cluster_name)
        logger.debug("list_container_instances response: %s", json.dumps(cluster_list_resp, default=datetime_handler))

        # If the event received is instance terminating...
        if 'LifecycleTransition' in message.keys():
            logger.debug("Autoscaling message %s", message['LifecycleTransition'])
            if message['LifecycleTransition'].find('autoscaling:EC2_INSTANCE_TERMINATING') > -1:

                # Get lifecycle hook name
                lifecycle_hook_name = message['LifecycleHookName']
                logger.debug("Setting lifecycle_hook_name: %s ", lifecycle_hook_name)

                # Check if there are any tasks running on the instance
                tasks_running, tmp_msg_append = check_container_instance_task_status(ec2_instance_id)

                if tmp_msg_append is not None:
                    message.update(tmp_msg_append)

                # If tasks are still running...
                if tasks_running:
                    msg_response = publish_to_sns(message, topic_arn)
                    logger.debug("publish_response: %s", json.dumps(msg_response, default=datetime_handler))

                # If tasks are NOT running...
                else:
                    logger.info('Setting lifecycle to complete; No tasks are running on instance')

                    response = asgClient.complete_lifecycle_action(
                        LifecycleHookName=lifecycle_hook_name,
                        AutoScalingGroupName=asg_group_name,
                        LifecycleActionResult='CONTINUE',
                        InstanceId=ec2_instance_id)

                    logger.debug(
                        "complete_lifecycle_action response: %s",
                        json.dumps(response, default=datetime_handler)
                    )
                    logger.info("Completed lifecycle hook action.")
        else:
            logger.info("Key 'LifecycleTransition' not found, skipping lifecycle hook action.")
