package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

type Node struct {
	Name       string
	IP         string
	Port       string
	Connection net.Conn
}

var nodes []Node

func findLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println("Error:", err)
		return ""
	}

	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}

	return ""

}

// Check if a connection to the node already exists
func connectionExists(name, ip, port string) bool {
	for _, node := range nodes {
		if node.IP == ip && node.Port == port {
			node.Name = name
			return true
		}
	}
	return false
}

func handleConfiguration() {

	file, err := os.Open("config.txt")
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer file.Close()

	fmt.Println("Successfully opened config file")

	scanner := bufio.NewScanner(file)
	scanner.Scan() // Skip the first line (number of nodes)

	localIP := findLocalIP()
	localParts := strings.Split(localIP, ":")

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")

		fmt.Println("Parts:", parts)

		if len(parts) != 3 {
			fmt.Println("Invalid line:", line)
			continue
		}

		node := Node{
			Name: parts[0],
			IP:   parts[1],
			Port: parts[2],
		}

		// Skip if the node is the current node
		if node.IP == localParts[0] && node.Port == localParts[1] {
			fmt.Println("Skipping self node", node.Name)
			nodes = append(nodes, node)
			continue
		}

		// Initiate a connection with the node if there isn't one already
		if !connectionExists(node.Name, node.IP, node.Port) {
			conn, err := net.Dial("tcp", node.IP+":"+node.Port)
			if err != nil {
				fmt.Println("Error connecting to node:", err)
				continue // Use continue instead of return to try the next node
			}

			// COMMENTING THIS OUT FOR NOW
			//defer conn.Close()

			// Store the connection and save the node
			node.Connection = conn
			nodes = append(nodes, node)

			fmt.Println("Successfully connected to node", node.Name)
		} else {
			fmt.Println("Connection already exists for node", node.Name)
		}

		fmt.Println("Successfully connected to node", node.Name)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}
}

func main() {

	/*
		Establish node connection to port
	*/

	arguments := os.Args
	if len(arguments) < 2 {
		fmt.Println("Please provide port number")
		return
	}

	PORT := ":" + arguments[1]
	l, err := net.Listen("tcp", PORT)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	fmt.Println("Server listening on port", arguments[1])
	defer l.Close()

	configTrigger := make(chan bool)
	connectionTrigger := make(chan bool)

	// Goroutine for checking user input
	go func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Print(">> ")
			text, _ := reader.ReadString('\n')
			if strings.TrimSpace(text) == "CONFIG" {
				configTrigger <- true
			}
		}
	}()

	// Goroutine for accepting connections
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("Connection accepted")

			// Get IP and port of the connection
			remoteAddr := conn.RemoteAddr().String()
			parts := strings.Split(remoteAddr, ":")
			remoteIP := parts[0]
			remotePort := parts[1]
			node := Node{
				IP:   remoteIP,
				Port: remotePort,
			}

			nodes = append(nodes, node)
			connectionTrigger <- true
		}
	}()

	for {
		select {
		case <-configTrigger:
			go handleConfiguration()
		case <-connectionTrigger:
			go handleConfiguration()
		}
	}

	return
}
