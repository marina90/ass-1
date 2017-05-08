import boto3
import botocore
import time
from botocore.exceptions import ClientError


class Manager:
    def __init__(self):
        self.should_terminate = False
        self.bucket_name = 'dsp1-bucket'
        self.sqs_names = ['Manager-Worker-queue', 'Worker-Manager-queue', 'Local-Manager-queue', 'Manager-Local-queue']
        self.sqs = boto3.resource(service_name='sqs')
        self.ec2 = boto3.resource(service_name='ec2')
        self.s3 = boto3.client(service_name='s3')
        self.s3_resource = boto3.resource(service_name='s3')
        self.start_sqs()

    def start_sqs(self):
        for i in range(len(self.sqs_names)):
            self.sqs.create_queue(QueueName=self.sqs_names[i])

    def start_workers(self, n):
        max_num_of_instances = 5
        user_data = '''#!/bin/bash
                                    apt-get update
                                    --nogpgcheck --skip-broken -y dos2unix glances screen gcc make python-devel python-setuptools python-pip git rubygems rpmbuild ruby-devel
                                    yum -y install --nogpgcheck ImageMagick
                                    yum -y install --nogpgcheck python2.7
                                    apt-get install python2.7
                                    git clone  https://github.com/marina90/ass-1
                                    sudo apt-get install imagemagick
                                    pip install boto3
                                    pip install botocore
                                    pip install pdfminer
                                    pip install wand
                                    pip install -r ass-1/requirements.txt
                                    python ass-1/MarTom/Worker.py
                                    '''
        current_amount_of_instances = 0
        instances = self.ec2.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'initializing', 'pending']}])
        for instance in instances:
            current_amount_of_instances += 1
        n = min(n, max_num_of_instances)
        if current_amount_of_instances < n:
            try:
                self.ec2.create_instances(ImageId='ami-bb6801ad', MinCount=1, MaxCount=n, InstanceType='t1.micro',
                                      KeyName='KeyPair', SecurityGroups=['default'],UserData=user_data)
            except Exception as e:
                print e
        time.sleep(5)
        instances = self.ec2.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'initializing', 'pending']}])
        for instance in instances:
            if instance.tags is None:
                instance.create_tags(Tags=[{'Key': 'Role', 'Value': 'Worker'}])

    def terminate_workers(self):
        instances = self.ec2.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'initializing', 'pending']}])
        for instance in instances:
            if instance.tags[0]['Value'] == 'Worker':
                self.ec2.instances.filter(InstanceIds=[instance.id]).terminate()

    def send(self, msg, local_name, num_of_lines, sqs_name):
        queue = self.sqs.get_queue_by_name(QueueName=sqs_name)
        attributes = {
            "LocalName": {
                "DataType": "String",
                "StringValue": local_name
            },
            "NumOfLines": {
                "DataType": "Number",
                "StringValue": str(num_of_lines)
            }
        }
        queue.send_message(MessageAttributes=attributes, MessageBody=msg)

    def wait_for_message(self, sqs_name):
        queue = self.sqs.get_queue_by_name(QueueName=sqs_name)
        for i in range(10):
            for message in queue.receive_messages(VisibilityTimeout=30, MessageAttributeNames=['All']):
                self.do(message)
                message.delete()

    def count_instances(self):
        instances = self.ec2.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'initializing', 'pending']}])
        instance_counter = 0
        for instance in instances:
            instance_counter += 1
        return instance_counter
    
    def send_terminate(self, local_name):
        self.terminate_workers()
        self.send('manager terminated', local_name, 0, self.sqs_names[3])
        self.should_terminate = True

    def do(self, message):
        local_name = message.message_attributes.get('LocalName').get('StringValue')
        print message.body
        if message.body == 'terminate':
            self.counter = self.count_instances()
            for i in range(self.counter - 1):
                self.send('terminate', local_name, 0, self.sqs_names[0])
        elif 'worker terminated' in message.body:
            self.counter -= 1
            if self.counter == 1:   #1 instance should remain for the manager
                self.send_terminate(local_name)
        else:
            parsed_message = message.body.split('\t')
            if parsed_message[0] == 'job':
                self.start_workers(int(float(parsed_message[2])))
                self.split_and_send(parsed_message[1], message)
            else:      #finished task
                self.makeHtmlFile(message)

    def split_and_send(self, input_file, message):
        self.s3_resource.meta.client.download_file(self.bucket_name, input_file, input_file)
        local_name = message.message_attributes.get('LocalName').get('StringValue')
        with open(input_file) as inputFile:
            numOfLines = sum(1 for _ in inputFile)
        with open(input_file) as inputFile:
            for line in inputFile:
                print line[:-1]
                self.send(line, local_name, numOfLines, self.sqs_names[0])

    def upload(self, to_upload):
        self.s3.upload_file(to_upload, self.bucket_name, to_upload)


    def makeHtmlFile(self, message):
        fileName = message.message_attributes.get('LocalName').get('StringValue') + '.html'
        exists = False
        try:
            self.s3_resource.Object(self.bucket_name, fileName).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                exists = False
            else:
                print e
        else:
            exists = True
        line = message.body
        numOfLines = int(message.message_attributes.get('NumOfLines').get('StringValue'))
        if not exists:
            with open(fileName, 'w') as outputFile:
                outputFile.write("<html><body>" + line + "<br>\n")
                n = 1
        else:
            with open(fileName, 'a') as outputFile:
                outputFile.write(line + "<br>\n")
            with open(fileName, 'r') as outputfile:
                n = sum(1 for _ in outputfile)
        if (n == numOfLines):
            with open(fileName, 'a') as outputFile:
                outputFile.write("</body></html>")
            self.upload(fileName)
            localName = message.message_attributes.get('LocalName').get('StringValue')
            numOfLines = int(message.message_attributes.get('NumOfLines').get('StringValue'))
            self.send("Done html", localName, numOfLines, self.sqs_names[3])
        else:
            self.upload(fileName)


def main():
    manager = Manager()
    manager.start_workers(3)
    while not manager.should_terminate:
        print 'switch'
        manager.wait_for_message(manager.sqs_names[1])
        manager.wait_for_message(manager.sqs_names[2])

if __name__ == "__main__":
    main()

main()