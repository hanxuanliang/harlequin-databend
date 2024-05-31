from databend_py import Client


def test_client():
    client = Client(host="localhost", port=8000, user="root", password="")
    res = client.execute("SELECT 1", with_column_types=True)
    print(res[0], type(res[0]), res)


if __name__ == "__main__":
    test_client()
