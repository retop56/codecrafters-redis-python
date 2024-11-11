# import socket  # noqa: F401
import asyncio


def connection_handler(reader, writer):
    return


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    # server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # server_socket.accept()  # wait for client
    server_socket = await asyncio.start_server(connection_handler, "localhost", 6379)
    async with server_socket:
        await server_socket.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
