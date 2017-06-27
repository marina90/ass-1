import urllib
from cStringIO import StringIO
import boto3
import time
from pdfminer.converter import HTMLConverter, TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from wand.image import Image
from wand.exceptions import WandException


class Worker :

    def __init__(self):
        self.connection = boto3.resource(service_name='sqs')
        self.active = True
        self.format_error_flag = False
        self.sqs_names =['Manager-Worker-queue', 'Worker-Manager-queue']
        self.s3_bucket_name = 'dsp1-bucket'


    def convert(self, msg):
        parsed_message = msg.split("\t")
        processed_data = "Incorrect message format "
        if parsed_message[0] == "ToImage":
            processed_data = self.convert_to_image(parsed_message[1])
        elif parsed_message[0] == "ToHTML":
            processed_data = self.convert_to_html(parsed_message[1])
        elif parsed_message[0] == "ToText":
            processed_data = self.convert_to_text(parsed_message[1])
        else:
            print "error : Incorrect message format "
        return processed_data

    def send_to_manager(self, msg, attributes):
        outQueue=self.connection.get_queue_by_name(QueueName=self.sqs_names[1])
        outQueue.send_message(MessageBody = msg, MessageAttributes = attributes)

    def upload(self, to_upload):
        s3 = boto3.client(service_name='s3')
        s3.upload_file(to_upload, self.s3_bucket_name, to_upload)
        return "https://s3.amazonaws.com/" + self.s3_bucket_name + '/' + to_upload

    def convert_to_image(self,msg):
        filename = msg.rsplit('/', 1)[1]
        filename = filename[:-4]
        try :
            with Image(filename=msg+"[0]", resolution=200) as img:
                img.save(filename=filename + ".png")
        except TypeError as e:
            self.format_error_flag = True
            return e.message
        except WandException as e:
            self.format_error_flag = True
            return "WandException error"
        except :
            self.format_error_flag = True
            return "Unexpected error"
        try:
            with Image(filename=filename + ".png") as img:
                img.resize(200, 150)
                img.save(filename=filename + ".png")
        except :
            self.format_error_flag = True
            return "Unexpected error"
        return filename + ".png"

    def convert_to_text(self,msg):
        testfile = urllib.URLopener()
        filename = msg.rsplit('/', 1)[1]
        filename = filename[:-4] + ".txt"
        try :
            testfile.retrieve(msg, filename)
        except IOError as e :
            self.format_error_flag = True
            return e
        except :
            self.format_error_flag = True
            return "Unexpected error"
        fp = file(filename, 'rb')
        rsrcmgr = PDFResourceManager()
        retstr = StringIO()
        codec = 'utf-8'
        laparams = LAParams()
        device = TextConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        for page in PDFPage.get_pages(fp):
            interpreter.process_page(page)
            data = retstr.getvalue()
            break
        upload_file=open(filename,'w')
        upload_file.write(data)
        return filename

    def convert_to_html(self,msg):
        testfile = urllib.URLopener()
        filename = msg.rsplit('/', 1)[1]
        filename = filename[:-4] + ".html"
        try :
            testfile.retrieve(msg, filename)
        except IOError as e:
            self.format_error_flag = True
            print "hey"
            return e
        except :
            self.format_error_flag = True
            return "Unexpected error"
        fp = file(filename, 'rb')
        rsrcmgr = PDFResourceManager()
        retstr = StringIO()
        codec = 'utf-8'
        laparams = LAParams()
        device = HTMLConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        for page in PDFPage.get_pages(fp):
            interpreter.process_page(page)
            data = retstr.getvalue()
            break
        upload_file = open(filename, 'w')
        upload_file.write(data)
        return filename


def main():
    readyToListen=False
    while not readyToListen:
        try:
            inQueue = worker.connection.get_queue_by_name(QueueName=worker.sqs_names[0])
            readyToListen=True
        except Exception as e:
            print e
            print "The queue: " + worker.sqs_names[0] + " is not available yet. Please wait."
            time.sleep(20)
    while worker.active:
        try:
            for message in inQueue.receive_messages(VisibilityTimeout=45, MessageAttributeNames=['All']):
                try:
                    attributes = message.message_attributes
                    if (message.body == "terminate"):
                        worker.active = False
                        terminate_message = 'worker terminated'
                        terminate_message = terminate_message.replace("\n", " ")
                        message.delete()
                        worker.send_to_manager(terminate_message, attributes)
                    else:  # message that need to be processed

                        file_to_upload = worker.convert(message.body)
                        if worker.format_error_flag or file_to_upload == "Incorrect message format ":
                            msg = message.body + "\t" + str(file_to_upload)
                            msg = msg.replace("\n", " ")
                            worker.send_to_manager(msg, attributes)
                            worker.format_error_flag = False
                        else:
                            url_of_s3 = worker.upload(file_to_upload)  # uploading the new data to the s3
                            parsed_message = message.body.split("\t")  # 0 - action ,1 - url
                            msg = parsed_message[1] + "\t" + url_of_s3 + "\t" + parsed_message[0]
                            msg = msg.replace("\n", " ")
                            worker.send_to_manager(msg, attributes)
                        message.delete()
                except:
                    worker.send_to_manager("error timeout", attributes)
                    message.delete()
        except Exception as e:
            print e


if __name__ == "__main__":
    worker = Worker()
    main()

main()
