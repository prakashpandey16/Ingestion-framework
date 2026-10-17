import logging
import os
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# Ensure environment variables are loaded before accessing config
load_dotenv()

from conf.config import config

def load_key_from_file():
    try:
        # Use environment variable directly, fallback to config
        key_file_path = os.environ.get("KEYFILEPATH", config["encryption"]["keyfilepath"])
        with open(key_file_path, "rb") as key_file:
            key = key_file.read()
            return key
    except Exception as e:
        raise ValueError(f"Error loading encryption key: {e}")

cipher_suite = Fernet(load_key_from_file())

def encrypt_value(value):
    if value is not None:
        logging.debug(f"Encrypting value: {value}")
        encrypted_value = cipher_suite.encrypt(value.encode())
        logging.debug(f"Encrypted value: {encrypted_value.decode()}")
        return encrypted_value.decode()
    return None

def decrypt_value(encrypted_value):
    if encrypted_value is not None:
        logging.debug(f"Decrypting value: {encrypted_value}")
        decrypted_value = cipher_suite.decrypt(encrypted_value.encode())
        logging.debug(f"Decrypted value: {decrypted_value.decode()}")
        return decrypted_value.decode()
    return None
