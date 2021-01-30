import pandas as pd
import pymongo
import json
from web3 import Web3
import csv

w3 = Web3(Web3.WebsocketProvider('wss://ropsten.infura.io/ws/v3/aedfdb9bade24481a2b829f32220850c'))
block = w3.eth.getBlock('latest')

address = "0x0551e27889EF5FB58b851558f35929463682bB52"

if block != None and block.transactions != None:
    for i in range(0,5):
        print(block.number)
        print("Nonce -",block.nonce.hex())
        for txHash in block.transactions:
            tx = w3.eth.getTransaction(txHash)
            send = (tx['from'])
            receiver = (tx['to'])
            ether = tx['value']
            if(send == address or receiver == address):
                print()
                print("Transaction is Found on Block -",block.number)
                print("Block Hash -",block.hash.hex())
                print("Transaction hash -",tx['hash'].hex())
                print("Timstamp -",block.timestamp)
                print("From: "+send+" To: "+receiver)
                print("Ether Value -",w3.fromWei(ether, "ether"))
                print()

                with open('Transactions.csv', mode='w') as csv_file:
                    fieldnames = ['Block', 'Block_Hash','Transaction_Hash', 'Timestamp','From','To','Ether']
                    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerow({'Block': block.number , 'Block_Hash':block.hash.hex() , 'Transaction_Hash':tx['hash'].hex() , 'Timestamp':block.timestamp , 'From':send , 'To':receiver ,'Ether':w3.fromWei(ether, "ether")})
                
                myclient = pymongo.MongoClient("mongodb+srv://<username>:<password>@cluster0.lwdx8.mongodb.net/<dbname>?retryWrites=true&w=majority")
                mydb = myclient["Transactions_Data"]
                mycol = mydb["user_transactions_details"]
                df = pd.read_csv("Transactions.csv")
                data = df.to_dict('records')
                x = mycol.insert_many(data)

            else:
                print("Transaction Not found")
                
        block=w3.eth.getBlock(block.number-1)
        print()

            