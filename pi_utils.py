import smtplib

def sendMail(sender, password, reciever, subject, body):
  smtp = "smtp.gmail.com"
  port = 465
  message = "Subject: {}\n\n{}\n\nCheers!".format(subject, body)

  if isinstance(reciever, list):
    print("Sending email to " + ", ".join(reciever) + " ...")
  else:
    print("Sending email to " + reciever + " ...")

  try:
    server = smtplib.SMTP_SSL(smtp, port)
    server.ehlo()
    server.login(sender, password)
    server.sendmail(sender, reciever, message.encode("utf-8"))
    server.close()
    print("Email sent!")
  except Exception as e:
    print("Something went wrong ...", e)
