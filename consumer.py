from kafka import KafkaConsumer
from tkinter import messagebox

maximo=input("Valor maximo :")
minimo=input("Valor minimo :")
consumer = KafkaConsumer('quickstart-events', bootstrap_servers=['localhost:9092'])
for message in consumer:
    if message.value > int(maximo) or message.value <int(minimo):
        messagebox.showinfo(message="No cumple los parametros", title="Alerta")

    print(message.value.decode("utf-8"))
