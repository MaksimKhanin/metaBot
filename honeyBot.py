from mt5_connector import mt5_connector
from teleInformer import TeleInformer
import dataPreprocesser

admiral_demo = mt5_connector(server='AdmiralMarkets-Demo',
                             login=40927195,
                             password='4740lpQbRYg7',
                             path="C:/metatraders/admiral/terminal64.exe",
                             timedelta=3)

Informer = TeleInformer(-414687545, '1155052470:AAELrvfzH_h2zMucZ7BTTaKeKS5lI01yrFg')