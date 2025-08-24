import socket

serverIP = '10.61.5.115'  
serverPort = 12345 

clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

clientSocket.connect((serverIP, serverPort))

def guess_number(client_socket):
    player_num = client_socket.recv(1024).decode()
    print(player_num)
    for round in range(1, 13):
        while True:
            if round <= 12:
                guess = input(f"รอบที่ {round}: ใส่ตัวเลข 6 หลักที่ทาย: ")
                
                if len(guess) != 6 or not guess.isdigit():
                    print("โปรดใส่ตัวเลข 6 หลักเท่านั้น")
                else:
                    break  
            else:
                break
            
        client_socket.send(guess.encode())
        result = client_socket.recv(1024).decode()

        if result == "ถูกต้อง ยินดีด้วย!" or result == "End Game":
            print(result)
            client_socket.close()
            break

        print(f"ผลลัพธ์รอบที่ {round}: {result}")

    client_socket.close()

guess_number(clientSocket)

