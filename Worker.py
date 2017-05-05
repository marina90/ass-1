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
    connection = boto3.resource(service_name='sqs')
    running = True
    error_occurred_while_trying_to_format = False
    '''
    listens to the sqs and extracts a message from the sqs ,then process's it by sending it to the run function 
    and then deletes it
    '''
    def pull_and_download(self):
        try:
            inQueue = self.connection.get_queue_by_name(QueueName="Manager-worker-queue")
        except :
            print "No sqs found waiting for one to open..."
            time.sleep (10)
            self.pull_and_download()
        while self.running == True :
            for message in inQueue.receive_messages(VisibilityTimeout=30, MessageAttributeNames=['All']):
                attributes = message.message_attributes
                if message.body == "terminate":
                    self.running = False
                    msg_to_sqs = 'worker terminated'
                    msg_to_sqs = msg_to_sqs.replace("\n", " ")
                    message.delete()
                    self.send_to_sqs(msg_to_sqs, attributes)
                else:       #message that need to be processed
                    processed_data = self.run_task(message.body)
                    if self.error_occurred_while_trying_to_format is True:
                        msg_to_sqs = message.body + "\t" + str(processed_data)
                        msg_to_sqs = msg_to_sqs.replace("\n", " ")
                        self.send_to_sqs(msg_to_sqs,attributes)
                        self.error_occurred_while_trying_to_format = False
                    else:
                        url_of_s3 = self.upload(processed_data)     #uploading the new data to the s3
                        parsed_message = message.body.split("\t")  # 0 - action ,1 - url
                        msg_to_sqs = parsed_message[1] + "\t" + url_of_s3 + "\t" + parsed_message[0]
                        msg_to_sqs = msg_to_sqs.replace("\n", " ")
                        self.send_to_sqs(msg_to_sqs, attributes)
                    message.delete()

    '''
    gets a complete msg (what to do and on which url ) and checks which action needs to be preformed 
    '''
    def  run_task(self,msg):
        parsed_message =msg.split("\t")
        processed_data = "wrong message format ,nothing changed "
        if parsed_message[0] == "ToImage":
            print "Trying to convert to Image"
            processed_data=self.convert_to_image(parsed_message[1])
        elif parsed_message[0] == "ToHTML":
            print "Trying to convert to HTML"
            processed_data =self.convert_to_html(parsed_message[1])
        elif parsed_message[0] == "ToText":
            print "Trying to convert to Text"
            processed_data =self.convert_to_text(parsed_message[1])
        else :
            print "error : Wrong message format "
        return processed_data

    '''
    gets a msg and attributes and sends the message with the attributes to the sqs 
    '''
    def  send_to_sqs(self, msg, attributes):
        outQueue=self.connection.get_queue_by_name(QueueName="Worker-manager-queue")
        outQueue.send_message(MessageBody=msg, MessageAttributes = attributes)

    '''
    gets a file and uploads it to s3 bucket
    '''
    def upload(self, to_upload) :
        s3 = boto3.client(service_name='s3')
        s3.upload_file(to_upload, "dsp1-bucket-gn", to_upload)
        return "https://s3.amazonaws.com/{}/{}".format("dsp1-bucket-gn", to_upload)

    '''
    gets a url and converts the message to an image file 
    '''
    def convert_to_image(self,msg):
        filename = msg.rsplit('/', 1)[1]
        filename = filename[:-4]
        try :
            with Image(filename=msg+"[0]", resolution=200) as img:
                img.save(filename=filename + ".png")
        except TypeError as e:
            return self.making_an_error_message(e.message)
        except WandException as e:
            return self.making_an_error_message(e.message)
        except:
            return self.making_an_error_message("unexpected error")
        try:
            with Image(filename=filename + ".png") as img:
                img.resize(200, 150)
                img.save(filename=filename + ".png")
        except:
            return self.making_an_error_message("unexpected error")
        return filename + ".png"

    '''
    gets a url and converts the message to a text file 
    '''
    def convert_to_text(self,msg):
        testfile = urllib.URLopener()
        filename = msg.rsplit('/', 1)[1]
        filename = filename[:-4] + ".txt"
        try:
            testfile.retrieve(msg, filename)
        except IOError as e :
            return self.making_an_error_message(e)
        except:
            return self.making_an_error_message("unexpected error")
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
        upload_file=open(filename, 'w')
        upload_file.write(data)
        return filename

    '''
    gets a url and converts the message to a html file 
    '''
    def convert_to_html(self,msg):
        testfile = urllib.URLopener()
        filename = msg.rsplit('/', 1)[1]
        filename = filename[:-4] + ".html"
        try :
            testfile.retrieve(msg, filename)
        except IOError as e:
            return self.making_an_error_message(e)
        except :
            return self.making_an_error_message("unexpected error")
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

    '''
    gets an exception and returns a message 
    '''
    def making_an_error_message(self ,e):
        self.error_occurred_while_trying_to_format = True
        return e



work = Worker()
work.pull_and_download()




