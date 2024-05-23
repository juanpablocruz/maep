package main

import "github.com/juanpablocruz/maep/internal/network"

func main() {
	nn := network.NewNetworkNode()

	err := nn.Join("/ip4/127.0.0.1/tcp/61092")
	if err != nil {
		panic(err)
	}
}
