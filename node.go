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

func main() {

	/*
		Establish node connection to port
	*/

	arguments := os.Args
	if len(arguments) < 1 {
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

	/*
		Handle configuration file
	*/

	file, err := os.Open("config.txt")
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Scan() // Skip the first line (number of nodes)

	localIP := findLocalIP()
	localParts := strings.Split(localIP, ":")


	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")

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
        if node.IP == localParts[0] && node.Port == localParts[1]{
            fmt.Println("Skipping self node", node.Name)
            nodes = append(nodes, node)
            continue
        }

		// Initiate a connection with the node
		conn, err := net.Dial("tcp", node.IP+":"+node.Port)
		if err != nil {
			fmt.Println("Error connecting to node:", err)
			return
		}
		defer conn.Close()

		// Store the connection and save the node
		node.Connection = conn

		nodes = append(nodes, node)

		fmt.Println("Successfully connected to node", node.Name)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}

	for {

	}

	return
}
