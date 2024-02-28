package main

import (
	"bufio"
	"container/heap"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

type Node struct {
	Name       string
	IP         string
	Port       string
	Connection net.Conn
}

type Transaction struct {
	TransactionID int
	Transaction   string
	Sender        string
	Priority      float32
	Deliverable   bool
	MessageType   string // "init", "proposed", or "final"
}

type PriorityQueue []Transaction

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest priority item, so we use less than here.
	return pq[i].Priority < pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }

func (pq *PriorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(Transaction))
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) Update(transaction Transaction) {
	for i := range *pq {
		if (*pq)[i].TransactionID == transaction.TransactionID {
			(*pq)[i] = transaction
			heap.Fix(pq, i)
			return
		}
	}
	heap.Push(pq, transaction)
}

var numNodes int
var parsedConfiguration [][]string // Each element is one line of the configuration file, split by spaces
var nodes []Node
var CONFIG_PATH string
var CURRENT_NODE string

var priority float32
var priorityMutex = &sync.Mutex{}

// var transactions []Transaction
var transactions PriorityQueue
var transactionMutex = &sync.Mutex{}

var transactionHandlers = make(map[int]chan Transaction)
var handlersMutex = &sync.Mutex{}

/*
Parse the configuration file and store the contents in parsedConfiguration
*/
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
		numNodes, err = strconv.Atoi(scanner.Text())
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

}

/*
Check if a connection to a node is nodes already exists
*/
func connectionExists(name, ip, port string) bool {
	for _, node := range nodes {
		if node.IP == ip && node.Port == port && node.Name == name {
			return true
		}
	}
	return false
}

/*
Establishes connections with all nodes in the configuration file
*/

func handleConfiguration() {
	// Iterate over the parsedConfiguration
	for _, config := range parsedConfiguration {
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

			// Start a goroutine to handle incoming transactions from the node
			go dispatchTransactions(conn)

			fmt.Println("Successfully connected to node:", nodeName)
		} else {
			fmt.Println("Connection already exists for node:", nodeName)
		}
	}
	fmt.Println("Node configuration completed! These are the currently connected nodes:")
	for _, node := range nodes {
		fmt.Println(node.Name, node.IP, node.Port)
	}
}

func generateTransactions() {
	// Execute the Python script and pipe its output
	cmd := exec.Command("python3", "-u", "gentx.py", "0.5")

	// Get current node number to append to priority
	substr := CURRENT_NODE[4:5]

	intVal, err := strconv.Atoi(substr)
	if err != nil {
		// Handle the error, perhaps the substring is not a valid integer
		fmt.Println("Error converting substring to int:", err)
		return
	}
	currNodeNum := float32(intVal) / 10

	// Create a pipe to read stdout from the cmd
	cmdOutput, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Println("Error creating StdoutPipe for Cmd", err)
		return
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		fmt.Println("Error starting Cmd", err)
		return
	}

	// Read the output
	transactionBuffer := bufio.NewReader(cmdOutput)

	for {
		output, _, err := transactionBuffer.ReadLine()

		fmt.Println("Received transaction:", string(output))

		generatedTransId := rand.Intn(10000) + 1

		priorityMutex.Lock()
		priority += 1
		generatedPriority := priority + currNodeNum
		priorityMutex.Unlock()

		if err != nil {
			if err == io.EOF {
				break // End of the output
			}
			fmt.Println("Error reading Cmd output:", err)
			return
		}

		// Convert the output to a Transaction object
		transaction := Transaction{
			TransactionID: generatedTransId,
			Transaction:   string(output),
			Sender:        CURRENT_NODE,
			Priority:      generatedPriority,
			Deliverable:   false,
			MessageType:   "init",
		}

		handlersMutex.Lock()
		ch, exists := transactionHandlers[transaction.TransactionID]
		if !exists {
			ch = make(chan Transaction, 50) // buffer size as needed
			transactionHandlers[transaction.TransactionID] = ch
			go senderTransactionHandler(ch, transaction) // Start a new goroutine for handling this transaction ID
		}
		handlersMutex.Unlock()

	}

	// Wait for the command to finish
	cmd.Wait()
}

func senderTransactionHandler(ch chan Transaction, initialTransaction Transaction) {

	proposedPriorities := []float32{}

	// Serialize the transaction data
	transactionData, err := json.Marshal(initialTransaction)

	if err != nil {
		fmt.Printf("Error marshaling transaction data: %v\n", err)
		return
	}

	proposedPriorities = append(proposedPriorities, initialTransaction.Priority)

	// Broadcast transaction to all nodes
	for _, node := range nodes {
		if node.Name == CURRENT_NODE {
			transactionMutex.Lock()
			transactions.Push(initialTransaction)
			transactionMutex.Unlock()
		} else {
			_, err = node.Connection.Write(transactionData)
			if err != nil {
				fmt.Printf("Error sending transaction to node %s: %v\n", node.Name, err)
			}
		}
	}

	for transaction := range ch {
		if transaction.MessageType == "priority" {
			proposedPriorities = append(proposedPriorities, transaction.Priority)

			// If all nodes have proposed a priority, calculate the final priority
			if len(proposedPriorities) == len(nodes) {
				finalPriority := float32(0.0)
				for _, priority := range proposedPriorities {
					if priority > finalPriority {
						finalPriority = priority
					}
				}

				// Update the transaction with the final priority
				transaction.Priority = finalPriority
				transaction.MessageType = "final"

				// Broadcast final priority to all nodes
				for _, node := range nodes {
					if node.Name == CURRENT_NODE {
						transaction.Deliverable = true
						transactionMutex.Lock()
						transactions.Update(transaction)
						transactionMutex.Unlock()
					} else {
						// Seriealize transaction data
						transactionData, err := json.Marshal(transaction)

						if err != nil {
							fmt.Printf("Error marshaling transaction data: %v\n", err)
							return
						}

						_, err = node.Connection.Write(transactionData)
						if err != nil {
							fmt.Printf("Error sending transaction to node %s: %v\n", node.Name, err)
						}
					}
				}

				// Clean up
				handlersMutex.Lock()
				delete(transactionHandlers, transaction.TransactionID)
				handlersMutex.Unlock()

				close(ch)
			}
		}
	}

	fmt.Println("Leaving transaction handler")
}

func dispatchTransactions(conn net.Conn) {
	defer conn.Close()

	var buffer [1024]byte // Adjust size as needed

	for {
		n, err := conn.Read(buffer[:])
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Error reading from connection: %v\n", err)
			}
			break
		}
		// fmt.Printf("Received: %s", string(buffer[:n]))

		var receivedTransaction Transaction

		// Deserialize the transaction data
		err = json.Unmarshal(buffer[:n], &receivedTransaction)
		if err != nil {
			fmt.Printf("Error unmarshaling transaction data: %v\n", err)
			return
		}

		// Clear the buffer
		buffer = [1024]byte{}

		// Broadcast message to all nodes with proposed priority
		if receivedTransaction.MessageType == "init" {
			// Update priority

			// Get current node number to append to priority
			substr := CURRENT_NODE[4:5]

			intVal, err := strconv.Atoi(substr)
			if err != nil {
				// Handle the error, perhaps the substring is not a valid integer
				fmt.Println("Error converting substring to int:", err)
				return
			}

			currNodeNum := float32(intVal) / 10

			priorityMutex.Lock()
			priority += 1
			proposedPriority := priority + currNodeNum
			priorityMutex.Unlock()

			receivedTransaction.Priority = proposedPriority
			receivedTransaction.MessageType = "priority"

			// Add the updated transaction with proposed priority to the priority queue
			fmt.Println("Aboout to add received transaction from", receivedTransaction.Sender, "to the priority queue")
			transactionMutex.Lock()
			transactions.Push(receivedTransaction)
			transactionMutex.Unlock()

			// Serialize the transaction data to send to original sender
			transactionData, err := json.Marshal(receivedTransaction)
			if err != nil {
				fmt.Printf("Error marshaling transaction data: %v\n", err)
				return
			}

			// Send the transaction data back to the original sender
			_, err = conn.Write(transactionData)
			if err != nil {
				fmt.Printf("Error sending transaction to original sender: %v\n", err)
			}
		} else if receivedTransaction.MessageType == "proposed" {
			// Send the transaction to the sender's transaction handler
			handlersMutex.Lock()
			ch, exists := transactionHandlers[receivedTransaction.TransactionID]
			handlersMutex.Unlock()
			if !exists {
				fmt.Printf("No transaction handler found for transaction ID %d\n", receivedTransaction.TransactionID)
			} else {
				ch <- receivedTransaction
			}
		} else if receivedTransaction.MessageType == "final" {
			// Update the transaction with the final priority
			receivedTransaction.Deliverable = true

			transactionMutex.Lock()
			transactions.Update(receivedTransaction)
			transactionMutex.Unlock()
		}
	}
	return
}

func testPriorityQueue() bool {
	// Create an empty priority queue
	var pq PriorityQueue
	heap.Init(&pq)

	// Add transactions with different priorities
	transactions := []Transaction{
		{TransactionID: 1, Priority: 5.0},
		{TransactionID: 2, Priority: 3.0},
		{TransactionID: 3, Priority: 7.0},
		{TransactionID: 4, Priority: 1.0},
		{TransactionID: 5, Priority: 2.0},
	}

	fmt.Println("Initial priority queue:", pq)

	for _, tx := range transactions {
		heap.Push(&pq, tx)
		fmt.Println("After push:", pq)
	}

	// Pop transactions from the priority queue and verify the order
	expectedOrder := []float32{1.0, 2.0, 3.0, 5.0, 7.0}
	for i := 0; pq.Len() > 0; i++ {
		tx := heap.Pop(&pq).(Transaction)
		if tx.Priority != expectedOrder[i] {
			fmt.Printf("Expected priority %.1f, got %.1f\n", expectedOrder[i], tx.Priority)
			return false
		}
		fmt.Println("After pop:", pq)
	}

	return true
}

func main() {
	/*
		Establish node connection to port
	*/
	arguments := os.Args
	if len(arguments) < 2 {
		fmt.Println("Incorrect number of arguments")
		return
	}

	CURRENT_NODE = arguments[1]
	CONFIG_PATH = arguments[2]
	var PORT string

	parseConfigurationFile()

	// Compares CURRENT_NODE to the confirguation file to find which port to listen on
	for line := range parsedConfiguration {
		if parsedConfiguration[line][0] == CURRENT_NODE { // If found, add self-connection to nodes list
			PORT = ":" + parsedConfiguration[line][2]
			var node = Node{
				Name: parsedConfiguration[line][0],
				IP:   parsedConfiguration[line][1],
				Port: PORT[1:],
			}
			nodes = append(nodes, node)
		}
	}

	if PORT == "" {
		fmt.Println("Node not found in configuration file")
	}

	// Establish listener to specified port
	l, err := net.Listen("tcp", PORT)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Server listening on port: ", PORT[1:])
	defer l.Close()

	// Handler for incoming connections. If a connection is established, add it to the nodes list
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

						go dispatchTransactions(conn)

						fmt.Printf("Added node %s to nodes list and awaiting transactions\n", nodeName)
					} else {
						fmt.Printf("Connection already exists for node %s\n", nodeName)
					}

					// Found a matching IP, no need to check further
					break
				}
			}
		}
	}()

	startTrigger := make(chan bool)
	evalTrigger := make(chan bool)

	// Handler for reading user input. If "CONFIG" is entered, trigger the configuration handler. If "EVAL" is entered, print the nodes list
	go func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			text, _ := reader.ReadString('\n')
			switch strings.TrimSpace(text) {
			case "START":
				startTrigger <- true
			case "EVAL":
				evalTrigger <- true
			}
		}
	}()

	for {
		select {
		case <-startTrigger:
			handleConfiguration()
			go generateTransactions()
		case <-evalTrigger:
			fmt.Println(len(transactions))
			for _, transaction := range transactions {
				fmt.Println(transaction)
			}
		}
	}
	return
}
