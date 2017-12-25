'''
Created on Nov 12, 2017

@author: nwe.nganesan
'''
# Import smtplib for the actual sending function
import smtplib
import sys
import os

from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

ImgFileName='/Users/nwe.nganesan/Desktop/images.png'
img_data = open(ImgFileName, 'rb').read()
msg = MIMEMultipart()
text = MIMEText("test")
msg.attach(text)
image = MIMEImage(img_data, name=os.path.basename(ImgFileName))
msg.attach(image)

server = smtplib.SMTP("smtp.gmail.com", 587)
server.ehlo()
server.starttls()

#server.login('nganesan@onemarketnetwork.com', 'Confident@01')
#server.sendmail('nganesan@onemarketnetwork.com', 'nganesan@onemarketnetwork.com', 'test')
server.login('nwe.nganesan@westfield.com', 'Confident@01')
server.sendmail('nwe.nganesan@westfield.com', 'nganesan@onemarketnetwork.com', msg.as_string())
server.close()
print('successfully sent the mail')
