
import boto3
import sys
import botocore
from boto import sqs
import time
from botocore.exceptions import ClientError
import boto
import boto.s3.connection
import math


class Manager:
    def __init__(self, secretKey, accessKey):
        self.secret = secretKey
        self.access = accessKey
        self.num_of_workers = 0
        self.should_terminate = False
        self.bucket_name = 'dsp1-bucket-ng'
        self.sqs_names = ['Manager-worker-queue', 'Worker-manager-queue', 'Local-Manager-queue', 'Manager-local-queue']
        self.sqs = boto3.resource(service_name='sqs', region_name='us-east-1', aws_access_key_id=accessKey,
                                  aws_secret_access_key=secretKey)
        self.queue = boto.sqs.connect_to_region('us-east-1', aws_access_key_id=accessKey,
                                                aws_secret_access_key=secretKey)
        self.ec2 = boto3.resource(service_name='ec2', region_name='us-east-1', aws_access_key_id=accessKey,
                                  aws_secret_access_key=secretKey)
        self.conn = boto.connect_ec2(aws_access_key_id=accessKey, aws_secret_access_key=secretKey)
        self.s3 = boto3.client(service_name='s3', region_name='us-east-1', aws_access_key_id=accessKey,
                               aws_secret_access_key=secretKey)
        self.s3_resource = boto3.resource(service_name='s3', region_name='us-east-1', aws_access_key_id=accessKey,
                                          aws_secret_access_key=secretKey)

    def create_sqs_queues(self):
        try :
            self.sqs.get_queue_by_name(QueueName=self.sqs_names[0])
        except :
            #print "no queue with this name"
            self.sqs.create_queue(QueueName=self.sqs_names[0])
        try :
            self.sqs.get_queue_by_name(QueueName=self.sqs_names[1])
        except :
            #print "no queue with this name"
            self.sqs.create_queue(QueueName=self.sqs_names[1])

    def terminate_workers(self):
        instances = self.ec2.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'initializing', 'pending']}])
        for instance in instances:
            if instance.tags[0]['Value'] == 'Worker':
                self.conn.terminate_instances(instance_ids=[instance.id])

    def create_workers(self, n):
        max_num_of_instances = 5
        #TODO: upload worker py
        self.num_of_workers = min(max_num_of_instances - 1, n)
        n += 1      # +1 instance of the manager
        ec2 = boto3.resource(service_name='ec2', region_name='us-east-1', aws_access_key_id=self.access,
                             aws_secret_access_key=self.secret)
        conn = boto.connect_ec2(aws_access_key_id=self.access, aws_secret_access_key=self.secret)

        instances = conn.get_all_instance_status()
        if len(instances) < n and len(instances) <= max_num_of_instances:
            ec2.create_instances(ImageId='ami-51792c38', MinCount=int(1), MaxCount=int(min(n, max_num_of_instances)),
                                 InstanceType='t1.micro')
        time.sleep(5)
        instances = ec2.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'initializing', 'pending']}])
        for instance in instances:
            # print test.tags
            if instance.tags is None:
                instance.create_tags(Tags=[{'Key': 'Role', 'Value': 'Worker'}])

    def send_message_with_attributes(self, msg, local_name, output_file_name, num_of_lines, sqs_name):
        queue = self.sqs.get_queue_by_name(QueueName=sqs_name)
        attributes = {
            "LocalName": {
                "DataType": "String",
                "StringValue": local_name
            },
            "OutputFileName": {
                "DataType": "String",
                "StringValue": output_file_name
            },
            "NumOfLines": {
                "DataType": "Number",
                "StringValue": str(num_of_lines)
            }
        }
        queue.send_message(MessageAttributes=attributes, MessageBody=msg)

    def listen(self, sqs_name):
        #all_messages = []
        #all_messages_body = []
        queue = self.queue.get_queue(sqs_name)
        messages = queue.get_messages(10, message_attributes='All')
        while len(messages) > 0:
            #all_messages.append(messages)
            for message in messages:
                if len(message.message_attributes) > 0:
                    self.process(message)
                    message.delete()
                #all_messages_body.append(message)
                messages = queue.get_messages(10)

    def process(self, message):
        local_name = message.message_attributes.get('LocalName').get('string_value')
        output = message.message_attributes.get('OutputFileName').get('string_value')
        if message._body == 'terminate':
            print 'Num of workers is: ' + str(self.num_of_workers)
            for i in range(self.num_of_workers):
                self.send_message_with_attributes('terminate', local_name, output, 0, self.sqs_names[0])
            '''self.listen(self.sqs_names[1])'''
        elif message._body == 'worker terminated':
            self.num_of_workers -= 1
            if self.num_of_workers == 0:
                self.terminate_workers()
                self.send_message_with_attributes('manager terminated', local_name, output, 0, self.sqs_names[3])
                self.should_terminate = True
        else:
            parsed_messsage = message._body.split('\t')
            if parsed_messsage[0] == 'job':
                self.create_workers(int(float(parsed_messsage[2])))
                self.parser(parsed_messsage[1], message)
            else:      #finished task
                self.makeHtml(message)

    def parser(self, input_file, message):
        self.s3_resource.meta.client.download_file('dsp1-bucket-ng', input_file, input_file)
        local_name = message.message_attributes.get('LocalName').get('string_value')
        output = message.message_attributes.get('OutputFileName').get('string_value')
        with open(input_file) as inputFile:
            numOfLines = sum(1 for _ in inputFile)
        with open(input_file) as inputFile:
            for line in inputFile:
                print line
                self.send_message_with_attributes(line, local_name, output, numOfLines, self.sqs_names[0])

    def upload(self, to_upload):
        self.s3.upload_file(to_upload, self.bucket_name, to_upload)

    def createHtmlPage(self, fileName, message):
        print "creating html "
        numOfLines = int(message.message_attributes.get('NumOfLines').get('string_value'))
        line = message._body
        content = "<html><body>" + line + "<br>\n"
        with open(fileName, 'w') as outputFile:
            outputFile.write(content)
        # save outputFile to s3
        if numOfLines == 1:
            self.finishHtmlPage(fileName, message)
        else:
            self.upload(fileName)

    def finishHtmlPage(self, fileName, message):
        with open(fileName, 'a') as outputFile:
            outputFile.write("</body></html>")
        # save outputFile to s3
        self.upload(fileName)
        localName = message.message_attributes.get('LocalName').get('string_value')
        fileName = message.message_attributes.get('OutputFileName').get('string_value') + '.html'
        numOfLines = int(message.message_attributes.get('NumOfLines').get('string_value'))
        self.send_message_with_attributes("Done html", localName, fileName, numOfLines, self.sqs_names[3])

    def addToHtmlPage(self, fileName, message):
        print "adding html "
        line = message._body
        numOfLines = int(message.message_attributes.get('NumOfLines').get('string_value'))
        with open(fileName, 'a') as outputFile:
            outputFile.write(line + "<br>\n")
        with open(fileName, 'r') as outputfile:
            num = sum(1 for _ in outputfile)
        # save outputFile to s3
        if num >= numOfLines:
            self.finishHtmlPage(fileName, message)
        else:
            self.upload(fileName)

    def makeHtml(self, message):
        fileName = message.message_attributes.get('LocalName').get('string_value') + '.html'
        exists = False
        try:
            self.s3_resource.Object('dsp1-bucket-ng', fileName).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                exists = False
            else:
                raise
        else:
            exists = True

        if not exists:
            self.createHtmlPage(fileName, message)
        else:
            self.addToHtmlPage(fileName, message)


def main():
    # TODO encrypt
    secretKey = 'bVX2TIrkidzDumwDIkPl+4QcsTqcN9xEEdxoSn3i'
    accessKey = 'AKIAIP6TU723P3W52SNQ'
    manager = Manager(secretKey, accessKey)
    manager.create_sqs_queues()
    while not manager.should_terminate:
        manager.listen('Local-Manager-queue')
        manager.listen('Worker-manager-queue')



if __name__ == "__main__":
    main()