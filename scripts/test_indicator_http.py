import json
import urllib.request


BASE_URL = "http://127.0.0.1:8000"


def request(method: str, path: str, token: str | None = None, payload: dict | None = None):
    url = f"{BASE_URL}{path}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"} if payload is not None else {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req) as resp:
        body = resp.read().decode("utf-8")
        status = resp.status
    return status, body


def main():
    status, body = request(
        "POST",
        "/auth/dev-login",
        payload={
            "email": "owner@devalor.com",
            "name": "Owner",
            "org_name": "Devalor Solucoes",
            "org_slug": "devalor_solucoes",
        },
    )
    print("Login status:", status)
    token = json.loads(body)["access_token"]
    status, body = request("GET", "/indicators", token=token)
    print("List status:", status)
    print("List body:", body[:200], "...")
    status, body = request("POST", "/indicators/27/run", token=token, payload={})
    print("Run status:", status)
    print("Run body:", body[:200], "...")


if __name__ == "__main__":
    main()
