import requests


def get_response_content(url) -> str:
    r: requests.Response = requests.get(url)
    if r.status_code == 200:
        return r.content.decode('utf-8')
    elif r.status_code == 403:
        raise RuntimeError('Wrong URL.')
    else:
        raise RuntimeError('Unexpected status code.')
