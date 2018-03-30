import inspect
from functools import wraps

import requests


def check_status(func):
    @wraps(func)
    def decorator(*args, method, url, **kwargs):
        if method.upper() not in ('GET', 'POST'):
            raise NotImplementedError('only GET or POST support!')
        try:
            response = func(*args, method=method, url=url, **kwargs)
        except Exception as err:
            raise err
        else:
            return response

    return decorator


def for_all_methods(decorator):
    @wraps(decorator)
    def decorate(cls):
        for method_name, method_obj in inspect.getmembers(cls, inspect.isfunction):
            if method_name.startswith('response_'):
                setattr(cls, method_name, decorator(method_obj))
        return cls
    return decorate


@for_all_methods(check_status)
class RequestsClient(object):
    def __init__(self):
        self.session = requests.Session()

    def response_json(self, *_, method, url, **kwargs):
        return self.session.request(method, url, **kwargs).json()

    def response_content(self, *_, method, url, **kwargs):
        return self.session.request(method, url, **kwargs).content

    def response_text(self, *_, method, url, **kwargs):
        return self.session.request(method, url, **kwargs).text


def main():
    requests_client = RequestsClient()
    # get
    print(requests_client.response_json(method='get', url='https://httpbin.org/get', params={'params': 'params'}))
    print(requests_client.response_content(method='get', url='https://httpbin.org/get', params={'params': 'params'}))
    print(requests_client.response_text(method='get', url='https://httpbin.org/get', params={'params': 'params'}))
    # post
    print(requests_client.response_json(method='post', url='https://httpbin.org/post',
                                        params={'params': 'params'}, data={'form_data': 'form_data'}))
    print(requests_client.response_content(method='post', url='https://httpbin.org/post',
                                           params={'params': 'params'}, data={'form_data': 'form_data'}))
    print(requests_client.response_text(method='post', url='https://httpbin.org/post',
                                        params={'params': 'params'}, data={'form_data': 'form_data'}))


if __name__ == '__main__':
    main()
