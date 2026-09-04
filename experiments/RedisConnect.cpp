#include <iostream>
#include <string>
#include <winsock2.h> // Native Windows Networking API

// Tell the compiler to link the Windows Socket library binaries
#pragma comment(lib, "ws2_32.lib")

class RawRedisClient
{
private:
    SOCKET connect_socket;

public:
    RawRedisClient() : connect_socket(INVALID_SOCKET) {}

    bool connect_to_server(const std::string &ip, int port)
    {
        WSADATA wsa_data;
        // Initialize Windows Sockets framework
        if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0)
        {
            std::cerr << "WSAStartup failed." << std::endl;
            return false;
        }

        // Create a streaming TCP socket
        connect_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (connect_socket == INVALID_SOCKET)
        {
            std::cerr << "Socket creation failed." << std::endl;
            WSACleanup();
            return false;
        }

        // Define target server endpoint coordinates
        sockaddr_in target_endpoint;
        target_endpoint.sin_family = AF_INET;
        target_endpoint.sin_port = htons(port);
        target_endpoint.sin_addr.s_addr = inet_addr(ip.c_str());

        // Attempt hand-shake connection over the network
        if (connect(connect_socket, (SOCKADDR *)&target_endpoint, sizeof(target_endpoint)) == SOCKET_ERROR)
        {
            std::cerr << "Failed to connect to Redis server." << std::endl;
            closesocket(connect_socket);
            WSACleanup();
            return false;
        }

        return true;
    }

    // Sends a raw string protocol buffer over the live pipe and reads the immediate feedback
    std::string execute_raw(const std::string &resp_command)
    {
        send(connect_socket, resp_command.c_str(), resp_command.length(), 0);

        char receive_buffer[512] = {0};
        int bytes_captured = recv(connect_socket, receive_buffer, 511, 0);

        if (bytes_captured > 0)
        {
            return std::string(receive_buffer, bytes_captured);
        }
        return "ERROR: No response from network layer.";
    }

    ~RawRedisClient()
    {
        if (connect_socket != INVALID_SOCKET)
        {
            closesocket(connect_socket);
            WSACleanup();
        }
    }
};

int main()
{
    RawRedisClient redis;

    std::cout << "Attempting connection to local Redis cluster node..." << std::endl;
    if (!redis.connect_to_server("127.0.0.1", 6379))
    {
        std::cerr << "Execution Aborted. Is Redis running on port 6379?" << std::endl;
        return 1;
    }
    std::cout << "Connected successfully!\n"
              << std::endl;

    // RESP Protocol explanation:
    // *1\r\n  -> Array size of 1 argument
    // $4\r\n  -> The next argument string is exactly 4 characters long
    // PING\r\n-> The argument value
    std::string ping_payload = "*1\r\n$4\r\nPING\r\n";

    std::cout << "Sending network payload: PING" << std::endl;
    std::string response = redis.execute_raw(ping_payload);

    // Clean visible output formatting to show carriage returns explicitly
    std::cout << "Raw Server Response Captured: ";
    for (char c : response)
    {
        if (c == '\r')
            std::cout << "\\r";
        else if (c == '\n')
            std::cout << "\\n";
        else
            std::cout << c;
    }
    std::cout << std::endl;

    return 0;
}