#include <iostream>
#include "buttonrpc-master/buttonrpc.hpp"

int main()
{
	buttonrpc client;
	client.as_client("127.0.0.1", 5555);
	int a = client.call<int>("foo", 2, 3).val();
	std::cout << "call foo result: " << a << std::endl;
	system("pause");
	return 0;
}