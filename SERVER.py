from socket import *
import socket
import threading
import random

host = socket.gethostname()
serverIP = socket.gethostbyname(host)  
serverPort = 12345
n = 1
clients = []
serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serverSocket.bind((serverIP, serverPort))
serverSocket.listen(5)  
print(f"เซิร์ฟเวอร์กำลังรอการเชื่อมต่อที่ IP:{serverIP} Port:{serverPort}")

def generate_random_num():
    return ''.join(random.sample('0123456789', 6))
random_num = generate_random_num()
print("เลขที่ถูก: ", random_num)

end = False  # เพิ่มตัวแปรนี้

def client(client_socket):
    global n
    player = n
    n += 1
    client_socket.send(f"คุณเป็นผู้เล่นที่ {player}\n".encode())
    print(f"เป็นผู้เล่นที่ {player}")

    for round in range(1, 13):
        data = client_socket.recv(1024).decode()

        if not data:
            break
        random_num == data
        correct_digits = sum(1 for a, b in zip(random_num, data) if a == b)
        correct_wrong_position = sum(1 for a, b in zip(random_num, data) if a != b and a in data)

        if random_num == data:
            check = f"ถูกต้อง ยินดีด้วย!"
            client_socket.send(check.encode())
            winner = f"ผู้ชนะได้แก่: ผู้เล่นที่ {player}"
            print(winner)
            client_socket.send(winner.encode())
            global end 
            end = True 
            for client in clients:
                if client != client_socket:
                    client.send(f"ผู้เล่นที่ {player} ชนะ ขอบคุณสำหรับการเข้าร่วม\nเฉลย {random_num}\nEnd Game".encode())
                    client_socket.close()
            break
        
        elif round == 12 and random_num != data:
            check = f"{correct_digits} {correct_wrong_position} ({data})คุณทายผิดเกินกว่าที่กำหนด 
            โปรดลองอีกครั้งในรอบถัดไป\n ผลลัพธ์ที่ถูกต้อง คือ {random_num} "
            print(f"รอบที่ {round}: ได้รับข้อมูลจากผู้เล่น {player}: {check}")
            client_socket.send(check.encode())

        elif not end:
            check = f"{correct_digits} {correct_wrong_position} ({data})"
            print(f"รอบที่ {round}: ได้รับข้อมูลจากผู้เล่น {player}: {check}")
            client_socket.send(check.encode())
    
    client_socket.close()

while True:
    
    if not end: 
        client_socket, addr = serverSocket.accept()
        print(f"เชื่อมต่อกับ {addr}")
        clients.append(client_socket)
        client_run = threading.Thread(target=client, args=(client_socket,))
        client_run.start()

