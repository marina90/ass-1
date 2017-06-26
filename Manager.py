import boto3
import botocore
import time
from botocore.exceptions import ClientError


class Manager:
    def __init__(self):
        self.num_of_workers = 0
        self.should_terminate = False
        self.bucket_name = 'ass1-bucket-gn'
        self.sqs_names = ['Manager-worker-queue', 'Worker-manager-queue', 'Local-Manager-queue', 'Manager-local-queue']
        self.sqs = boto3.resource(service_name='sqs')
        self.ec2 = boto3.resource(service_name='ec2')
        self.s3 = boto3.client(service_name='s3')
        self.s3_resource = boto3.resource(service_name='s3')

    def create_sqs_queues(self):
        try:
            self.sqs.get_queue_by_name(QueueName=self.sqs_names[0])
        except:
            self.sqs.create_queue(QueueName=self.sqs_names[0])
        try:
            self.sqs.get_queue_by_name(QueueName=self.sqs_names[1])
        except:
            self.sqs.create_queue(QueueName=self.sqs_names[1])

    def terminate_workers(self):
        instances = self.ec2.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'initializing', 'pending']}])
        for instance in instances:
            if instance.tags[0]['Value'] == 'Worker':
                self.ec2.instances.filter(InstanceIds=[instance.id]).terminate()

    def create_workers(self, n):
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
                                    
                                    python ass-1/Worker.py
                                    '''
        self.num_of_workers = min(max_num_of_instances - 1, n)
        n += 1      # +1 instance of the manager
        current_amount_of_instances = 0
        instances = self.ec2.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'initializing', 'pending']}])
        for instance in instances:
            current_amount_of_instances += 1
        if current_amount_of_instances < n and current_amount_of_instances <= max_num_of_instances:
            self.ec2.create_instances(ImageId='ami-8e4b6a98', MinCount=int(1), MaxCount=int(1), InstanceType='t1.micro',
                                      KeyName='KeyPair', SecurityGroups=['default'],UserData=user_data)
        time.sleep(5)
        instances = self.ec2.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'initializing', 'pending']}])
        for instance in instances:
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
        queue = self.sqs.get_queue_by_name(QueueName=sqs_name)
        for i in range(10):
            for message in queue.receive_messages(VisibilityTimeout=30, MessageAttributeNames=['All']):
                self.process(message)
                message.delete()

    def process(self, message):
        local_name = message.message_attributes.get('LocalName').get('StringValue')
        output = message.message_attributes.get('OutputFileName').get('StringValue')
        if message.body == 'terminate':
            print 'Num of workers is: ' + str(self.num_of_workers)
            for i in range(1, self.num_of_workers):
                self.send_message_with_attributes('terminate', local_name, output, 0, self.sqs_names[0])
        elif message.body == 'worker terminated':
            self.num_of_workers -= 1
            if self.num_of_workers == 0:
                self.terminate_workers()
                self.send_message_with_attributes('manager terminated', local_name, output, 0, self.sqs_names[3])
                self.should_terminate = True
        else:
            parsed_message = message.body.split('\t')
            if parsed_message[0] == 'job':
                self.create_workers(int(float(parsed_message[2])))
                self.parser(parsed_message[1], message)
            else:      #finished task
                self.makeHtml(message)

    def parser(self, input_file, message):
        self.s3_resource.meta.client.download_file(self.bucket_name, input_file, input_file)
        local_name = message.message_attributes.get('LocalName').get('StringValue')
        output = message.message_attributes.get('OutputFileName').get('StringValue')
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
        numOfLines = int(message.message_attributes.get('NumOfLines').get('StringValue'))
        line = message.body
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
        localName = message.message_attributes.get('LocalName').get('StringValue')
        fileName = message.message_attributes.get('OutputFileName').get('StringValue') + '.html'
        numOfLines = int(message.message_attributes.get('NumOfLines').get('StringValue'))
        self.send_message_with_attributes("Done html", localName, fileName, numOfLines, self.sqs_names[3])

    def addToHtmlPage(self, fileName, message):
        print "adding html "
        line = message.body
        numOfLines = int(message.message_attributes.get('NumOfLines').get('StringValue'))
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
        fileName = message.message_attributes.get('LocalName').get('StringValue') + '.html'
        exists = False
        try:
            self.s3_resource.Object(self.bucket_name, fileName).load()
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
    manager = Manager()
    manager.create_sqs_queues()
    while not manager.should_terminate:
        manager.listen('Local-Manager-queue')
        manager.listen('Worker-manager-queue')

if __name__ == "__main__":
    main()