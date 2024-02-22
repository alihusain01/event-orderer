package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type Node struct {
	Name       string
	IP         string
	Port       string
	Connection net.Conn
}

var numNodes int
var parsedConfiguration [][]string // Each element is one line of the configuration file, split by spaces
var nodes []Node
var CONFIG_PATH string
var CURRENT_NODE string

func parseConfigurationFile() {
	file, err := os.Open(CONFIG_PATH)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// Advance the scanner to the first line and save it as the number of nodes
	if scanner.Scan() {
		numNodes, err := strconv.Atoi(scanner.Text())
		fmt.Println("Number of nodes: ", numNodes)
		if err != nil {
			fmt.Println("Error converting number of nodes: ", err)
			return
		}
	} else {
		if err := scanner.Err(); err != nil {
			fmt.Println("Error reading first line: ", err)
		} else {
			fmt.Println("File is empty")
		}
		return
	}

	// Process the rest of the file
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		parsedConfiguration = append(parsedConfiguration, parts)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading configuration file: ", err)
		return
	}

	fmt.Println("Parsed configuration file: ", parsedConfiguration)
}

// Check if a connection to the node already exists
func connectionExists(name, ip, port string) bool {
	for _, node := range nodes {
		if node.IP == ip && node.Port == port && node.Name == name {
			return true
		}
	}
	return false
}

func handleConfiguration() {

	// Iterate over the parsedConfiguration
	for _, config := range parsedConfiguration {
		fmt.Println("Config:", config)
		if len(config) != 3 {
			fmt.Println("Invalid configuration:", config)
			continue
		}

		nodeName := config[0]
		nodeIP := config[1]
		nodePort := config[2]

		// Skip if the node is the current node
		if nodeName == CURRENT_NODE {
			fmt.Println("Skipping self node:", nodeName)
			continue
		}

		// Check if a connection to the node already exists
		if !connectionExists(nodeName, nodeIP, nodePort) {
			fmt.Println("Attempting connection to node:", nodeName, "at", nodeIP+":"+nodePort)
			conn, err := net.Dial("tcp", nodeIP+":"+nodePort)
			if err != nil {
				fmt.Println("Error connecting to node:", nodeName, err)
				continue // Use continue instead of return to try the next node
			}

			// Store the connection and save the node
			node := Node{
				Name:       nodeName,
				IP:         nodeIP,
				Port:       nodePort,
				Connection: conn,
			}
			nodes = append(nodes, node)

			fmt.Println("Successfully connected to node:", nodeName)
		} else {
			fmt.Println("Connection already exists for node:", nodeName)
		}
	}

	fmt.Println("Node configuration complete!")
}

func main() {

	/*
		Establish node connection to port
	*/

	// fmt.Println(os.Args[0])
	// fmt.Println(os.Args[1])
	// fmt.Println(os.Args[2])

	arguments := os.Args
	if len(arguments) < 2 {
		fmt.Println("Incorrect number of arguments")
		return
	}

	CURRENT_NODE = arguments[1]
	CONFIG_PATH = arguments[2]
	var PORT string

	parseConfigurationFile()

	// Establish a connection to the port
	for line := range parsedConfiguration {
		if parsedConfiguration[line][0] == CURRENT_NODE {
			PORT = ":" + parsedConfiguration[line][2]
			var node = Node{
				Name: parsedConfiguration[line][0],
				IP:   parsedConfiguration[line][1],
				Port: PORT[1:],
			}
			nodes = append(nodes, node)
			// fmt.Println("PORT: ", PORT)
		}
	}

	if PORT == "" {
		fmt.Println("Node not found in configuration file")
	}

	l, err := net.Listen("tcp", PORT)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("Server listening on port: ", PORT[1:])
	defer l.Close()

	configTrigger := make(chan bool)
	evalTrigger := make(chan bool)

	// Goroutine for handling incoming connections
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer conn.Close()

			remoteIP := conn.RemoteAddr().(*net.TCPAddr).IP.String()
			fmt.Println("New connection from", remoteIP)

			// Check if the IP matches any in the parsedConfiguration and append if so
			for _, config := range parsedConfiguration {
				nodeName := config[0]
				nodeIP := config[1]
				nodePort := config[2]

				// Check if the remote IP matches the node IP in the configuration
				if remoteIP == nodeIP {
					fmt.Println("Matching IP found in configuration:", remoteIP)

					// Ensure no duplicate connections
					if !connectionExists(nodeName, nodeIP, nodePort) {
						node := Node{
							Name:       nodeName,
							IP:         nodeIP,
							Port:       nodePort,
							Connection: conn,
						}
						nodes = append(nodes, node)
						fmt.Printf("Added node %s to nodes list\n", nodeName)
					} else {
						fmt.Printf("Connection already exists for node %s\n", nodeName)
					}

					// Found a matching IP, no need to check further
					break
				}
			}
		}
	}()

	// Goroutine for checking user input
	go func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			text, _ := reader.ReadString('\n')
			switch strings.TrimSpace(text) {
			case "CONFIG":
				configTrigger <- true
				// fmt.Print("configTrigger sent\n")
			case "EVAL":
				evalTrigger <- true
			}
		}
	}()

	for {
		select {
		case <-configTrigger:
			// fmt.Println("configTrigger received")
			go handleConfiguration()
			// fmt.Println("Configuration complete")
		case <-evalTrigger:
			// fmt.Println("evalTrigger received")
			if len(nodes) == 0 {
				fmt.Println("No nodes to evaluate")
			} else {
				for _, node := range nodes {
					fmt.Println(node.Name, node.IP, node.Port)
				}
			}
		}
	}

	return
}
