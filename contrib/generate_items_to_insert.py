import string
import random


def generate_random_string(size=16,
                           chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for _ in xrange(size))


def generate_record(**kwargs):
    author = kwargs.get("author") or generate_random_string()
    message_id = kwargs.get("message_id") or generate_random_string()
    subject = kwargs.get("subject") or generate_random_string()
    date_time = kwargs.get("date_time") or generate_random_string(
        chars="123456789", size=1
    ) + generate_random_string(
        chars=string.digits
    )
    body = kwargs.get("body") or generate_random_string(size=1024)

    return (
        '{{"Author":{{"S":"{}"}},"MessageId":{{"S":"{}"}},'
        '"Subject":{{"S":"{}"}},"DateTime":{{"N":{}}},'
        '"Body":{{"S":"{}"}}}}'.format(
            author, message_id, subject, date_time, body)
    )

if __name__ == '__main__':
    author = "Author"
    with open('output.txt', 'w') as f:
        f.truncate()
        n = 10000
        for x in xrange(n):
            f.write(generate_record())
            f.write("\n")
