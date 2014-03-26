import requests


def get_items():
    for i in xrange(100000):
        yield(json % i)
        print i


if __name__ == '__main__':
    data = "01234567890" * 100
    json = '{"id": {"N": %d}, "field": {"S": "hello"}, "data": {"S":"' + data + '"}}\n'

r = requests.post('http://localhost:9999', data=get_items(),
                  headers={'Content-Type': 'application/json'})

print r.content
