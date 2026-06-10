from tiled.client import from_uri
from tiled.client import from_profile

c = from_uri("http://127.0.0.1:8000", api_key="secret")
# c = from_uri("http://127.0.0.1:8000")
# c = from_profile("demo")


print(c)


# pixi run python3

# from tiled.client import from_uri

# for the script: c = from_uri("http://127.0.0.1:8000", api_key="secret")
# for the alice version: c = from_uri("http://127.0.0.1:8000")
# c = from_uri("http://127.0.0.1:8000", api_key=os.environ["TILED_API_KEY"])


# to write array: c.write_array(item, key)

# to write data frame: df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
# c.write_dataframe(df, key)

# to read: c["key"].read()

# To store the API Key: export TILED_API_KEY=$(pixi run tiled api_key create)


