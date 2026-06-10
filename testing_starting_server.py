from tiled.client import simple

c = simple('./tiled/storage', api_key="secret", port=8000)




print(c)
input("Press Enter to stop the server...")

# pixi run python3 ./testing_starting_server.py




# ALICE_PASSWORD=secret1 pixi run tiled serve config example_configs/toy_authentication.yml


# CARA_PASSWORD=secret1 pixi run tiled serve config example_configs/multiple_providers.yml