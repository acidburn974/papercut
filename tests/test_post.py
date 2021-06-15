from nntplib import NNTP

# https://grokbase.com/t/python/python-list/0153k2drmp/nntp-post-a-file
# https://docs.python.org/3.8/library/nntplib.html#methods
# https://docs.python.org/3/tutorial/inputoutput.html

print("start")

text = '''From: Daniel <Daniel.Kinnaer at Advalvas.be>
Newsgroups: test.test
Subject: Testing NNTP using Python
X-Newsreader: Python home brew
MIME-Version: 1.0
Content-Type: text/plain; charset=utf8
Content-Transfer-Encoding: 7bit
Lines: 3

This is a testfile, please disregard. 
Posting this file using Python 
end of message
'''

f = open("article.txt", "r")
#print(f.read())

c = NNTP("localhost")
#c.group("slashthd.general")
c.post(bytes(text, "utf-8"))
#c.quit()

print("done...")

