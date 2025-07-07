import sys
import json
import os
import requests
import time
import threading
import queue
import subprocess
import random
from datetime import datetime
import numpy as np
import platform
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                            QLabel, QLineEdit, QPushButton, QCheckBox, QDoubleSpinBox,
                            QGridLayout, QGroupBox, QMessageBox, QSlider, QTabWidget,
                            QTableWidget, QTableWidgetItem, QHeaderView, QAbstractItemView,
                            QDialog, QFormLayout, QDialogButtonBox, QSplitter)
from PyQt5.QtCore import Qt, QTimer, pyqtSignal, QObject, QUrl
from PyQt5.QtGui import QFont, QIcon, QColor, QBrush
import pyqtgraph as pg

# Пул сессий для HTTP запросов
session_pool = {}
MAX_SESSIONS = 5

# Глобальный семафор для ограничения одновременных запросов
api_semaphores = {
    'dexscreener': threading.Semaphore(3),
    'mexc': threading.Semaphore(3),
    'gate': threading.Semaphore(3)
}

# Кэш для результатов API
class APICache:
    def __init__(self, cache_duration=2.0):
        self.cache = {}
        self.cache_duration = cache_duration
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            if key in self.cache:
                data, timestamp = self.cache[key]
                if time.time() - timestamp < self.cache_duration:
                    return data
                else:
                    del self.cache[key]
            return None

    def set(self, key, data):
        with self.lock:
            self.cache[key] = (data, time.time())

api_cache = APICache(cache_duration=2.0)

# Глобальный планировщик запросов
class RequestScheduler:
    def __init__(self, requests_per_second=5):
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0
        self.lock = threading.Lock()

    def acquire(self):
        with self.lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.min_interval:
                time.sleep(self.min_interval - time_since_last)
            self.last_request_time = time.time()

request_scheduler = RequestScheduler(requests_per_second=5)

def get_session(api_name):
    """Получить сессию из пула или создать новую с случайными заголовками"""
    global session_pool
    
    if api_name not in session_pool:
        session_pool[api_name] = []
    
    if len(session_pool[api_name]) < MAX_SESSIONS:
        session = requests.Session()
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'
        ]
        session.headers.update({
            'User-Agent': random.choice(user_agents),
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        })
        session_pool[api_name].append(session)
    
    return random.choice(session_pool[api_name])

def get_dexscreener_price_with_retry(token_address, coin_name, max_retries=3, backoff_factor=0.5):
    """Получить цену с Dexscreener с повторными попытками"""
    cache_key = f"dexscreener:{token_address}"
    cached_data = api_cache.get(cache_key)
    if cached_data is not None:
        return cached_data

    url = f"https://api.dexscreener.com/latest/dex/search/?q={token_address}"
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            request_scheduler.acquire()
            with api_semaphores['dexscreener']:
                session = get_session('dexscreener')
                response = session.get(url, timeout=10)
                
                if response.status_code == 429:
                    wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                    print(f"[{coin_name}] Ограничение запросов Dexscreener, ожидание {wait_time:.2f} сек")
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                
                if response.status_code != 200:
                    print(f"[{coin_name}] Ошибка Dexscreener: {response.status_code}")
                    if retry_count < max_retries - 1:
                        wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                        time.sleep(wait_time)
                    retry_count += 1
                    continue
                
                data = response.json()
                
                if "pairs" in data and data["pairs"]:
                    most_liquid = max(data["pairs"], key=lambda x: float(x.get("liquidity", {}).get("usd", 0) or 0))
                    price = float(most_liquid["priceUsd"])
                    dex_name = most_liquid["dexId"]
                    result = (price, dex_name)
                    api_cache.set(cache_key, result)
                    return result
                else:
                    print(f"[{coin_name}] Токен не найден на Dexscreener")
                    return None, None
        
        except requests.exceptions.Timeout:
            print(f"[{coin_name}] Таймаут запроса к Dexscreener")
            if retry_count < max_retries - 1:
                wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                time.sleep(wait_time)
            retry_count += 1
        except requests.exceptions.ConnectionError:
            print(f"[{coin_name}] Ошибка соединения с Dexscreener")
            if retry_count < max_retries - 1:
                wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                time.sleep(wait_time)
            retry_count += 1
        except json.JSONDecodeError:
            print(f"[{coin_name}] Ошибка декодирования JSON от Dexscreener")
            if retry_count < max_retries - 1:
                wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                time.sleep(wait_time)
            retry_count += 1
        except Exception as e:
            print(f"[{coin_name}] Ошибка при получении данных с Dexscreener: {str(e)}")
            return None, None
    
    print(f"[{coin_name}] Превышено максимальное количество попыток для Dexscreener")
    return None, None

def get_mexc_futures_prices_with_retry(max_retries=3, backoff_factor=0.5):
    """Получить цены всех фьючерсов с MEXC одним запросом"""
    cache_key = "mexc:all_futures"
    cached_data = api_cache.get(cache_key)
    if cached_data is not None:
        return cached_data

    url = "https://contract.mexc.com/api/v1/contract/ticker"
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            request_scheduler.acquire()
            with api_semaphores['mexc']:
                session = get_session('mexc')
                response = session.get(url, timeout=10)
                
                if response.status_code == 429:
                    wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                    print(f"[MEXC] Ограничение запросов, ожидание {wait_time:.2f} сек")
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                
                if response.status_code != 200:
                    print(f"[MEXC] Ошибка: {response.status_code}")
                    if retry_count < max_retries - 1:
                        wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                        time.sleep(wait_time)
                    retry_count += 1
                    continue
                
                data = response.json()
                
                if data.get("success", False):
                    prices = {item["symbol"]: float(item["lastPrice"]) for item in data["data"]}
                    api_cache.set(cache_key, prices)
                    return prices
                else:
                    error_msg = data.get("message", "Неизвестная ошибка")
                    if "frequently" in error_msg.lower():
                        wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                        print(f"[MEXC] Ошибка: {error_msg}, ожидание {wait_time:.2f} сек")
                        time.sleep(wait_time)
                        retry_count += 1
                        continue
                    
                    print(f"[MEXC] Ошибка: {error_msg}")
                    return None
        
        except requests.exceptions.Timeout:
            print(f"[MEXC] Таймаут запроса")
            if retry_count < max_retries - 1:
                wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                time.sleep(wait_time)
            retry_count += 1
        except requests.exceptions.ConnectionError:
            print(f"[MEXC] Ошибка соединения")
            if retry_count < max_retries - 1:
                wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                time.sleep(wait_time)
            retry_count += 1
        except json.JSONDecodeError:
            print(f"[MEXC] Ошибка декодирования JSON")
            if retry_count < max_retries - 1:
                wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                time.sleep(wait_time)
            retry_count += 1
        except Exception as e:
            print(f"[MEXC] Ошибка: {str(e)}")
            return None
    
    print(f"[MEXC] Превышено максимальное количество попыток")
    return None

def get_gate_futures_prices_with_retry(max_retries=3, backoff_factor=0.5):
    """Получить цены всех фьючерсов с Gate.io одним запросом"""
    cache_key = "gate:all_futures"
    cached_data = api_cache.get(cache_key)
    if cached_data is not None:
        return cached_data

    url = "https://api.gateio.ws/api/v4/futures/usdt/tickers"
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            request_scheduler.acquire()
            with api_semaphores['gate']:
                session = get_session('gate')
                response = session.get(url, timeout=10)
                
                if response.status_code == 429:
                    wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                    print(f"[Gate] Ограничение запросов, ожидание {wait_time:.2f} сек")
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                
                if response.status_code != 200:
                    print(f"[Gate] Ошибка: {response.status_code}")
                    if retry_count < max_retries - 1:
                        wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                        time.sleep(wait_time)
                    retry_count += 1
                    continue
                
                data = response.json()
                
                prices = {item["contract"]: float(item["last"]) for item in data}
                api_cache.set(cache_key, prices)
                return prices
        
        except requests.exceptions.Timeout:
            print(f"[Gate] Таймаут запроса")
            if retry_count < max_retries - 1:
                wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                time.sleep(wait_time)
            retry_count += 1
        except requests.exceptions.ConnectionError:
            print(f"[Gate] Ошибка соединения")
            if retry_count < max_retries - 1:
                wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                time.sleep(wait_time)
            retry_count += 1
        except json.JSONDecodeError:
            print(f"[Gate] Ошибка декодирования JSON")
            if retry_count < max_retries - 1:
                wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                time.sleep(wait_time)
            retry_count += 1
        except Exception as e:
            print(f"[Gate] Ошибка: {str(e)}")
            return None
    
    print(f"[Gate] Превышено максимальное количество попыток")
    return None

try:
    from PyQt5.QtMultimedia import QSoundEffect
    USE_SOUND_EFFECT = True
except ImportError:
    try:
        from PyQt5.QtMultimedia import QSound
        USE_SOUND_EFFECT = False
    except ImportError:
        USE_SOUND_EFFECT = None

CONFIG_FILE = 'gpt_spread_config.json'

class SignalEmitter(QObject):
    update_data = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

class AddCoinDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Добавить новую монету")
        self.setMinimumWidth(400)
        
        layout = QFormLayout(self)
        
        self.symbol_input = QLineEdit()
        self.symbol_input.setPlaceholderText("Например: BTC_USDT")
        layout.addRow("Символ фьючерса (MEXC/Gate):", self.symbol_input)
        
        self.contract_input = QLineEdit()
        self.contract_input.setPlaceholderText("Например: 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599 (WBTC)")
        layout.addRow("Контракт токена:", self.contract_input)
        
        self.threshold_input = QDoubleSpinBox()
        self.threshold_input.setRange(-100, 100)
        self.threshold_input.setValue(2.0)
        self.threshold_input.setDecimals(2)
        self.threshold_input.setSingleStep(0.1)
        layout.addRow("Сигнал при спреде (%):", self.threshold_input)
        
        self.interval_input = QDoubleSpinBox()
        self.interval_input.setRange(1.0, 60)
        self.interval_input.setValue(5.0)
        self.interval_input.setDecimals(1)
        layout.addRow("Интервал обновления (сек):", self.interval_input)
        
        self.enable_sound = QCheckBox("Включить звуковой сигнал")
        self.enable_sound.setChecked(True)
        layout.addRow("", self.enable_sound)
        
        self.center_zero = QCheckBox("Центрировать график спреда вокруг нуля")
        self.center_zero.setChecked(True)
        layout.addRow("", self.center_zero)
        
        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        layout.addRow(self.button_box)
        
    def get_data(self):
        return {
            "symbol": self.symbol_input.text().strip().upper(),
            "contract": self.contract_input.text().strip(),
            "threshold": self.threshold_input.value(),
            "interval": self.interval_input.value(),
            "enable_sound": self.enable_sound.isChecked(),
            "center_zero": self.center_zero.isChecked()
        }
        
    def validate(self):
        if not self.symbol_input.text().strip():
            QMessageBox.warning(self, "Ошибка", "Введите символ фьючерса")
            return False
        if not self.contract_input.text().strip():
            QMessageBox.warning(self, "Ошибка", "Введите контракт токена")
            return False
        return True
    
    def accept(self):
        if self.validate():
            super().accept()

class CoinMonitor(QObject):
    data_updated = pyqtSignal(dict)
    spread_changed = pyqtSignal(float, float, str)
    
    def __init__(self, coin_data, mexc_price_fetcher, gate_price_fetcher):
        super().__init__()
        self.data = coin_data
        self.symbol = coin_data["symbol"]
        self.contract = coin_data["contract"]
        self.threshold = coin_data["threshold"]
        self.interval = max(coin_data["interval"], 1.0)
        self.enable_sound = coin_data["enable_sound"]
        self.center_zero = coin_data["center_zero"]
        self.mexc_price_fetcher = mexc_price_fetcher
        self.gate_price_fetcher = gate_price_fetcher
        
        self.coin_name = self.symbol.split("_")[0] if "_" in self.symbol else self.symbol
        
        self.dex_prices = []
        self.mexc_prices = []
        self.gate_prices = []
        self.mexc_spreads = []
        self.gate_spreads = []
        self.timestamps = []
        self.last_update_time = ""
        self.last_mexc_percent = 0.0
        self.last_gate_percent = 0.0
        
        self.is_running = False
        self.thread = None
    
    def to_dict(self):
        return {
            "symbol": self.symbol,
            "contract": self.contract,
            "threshold": self.threshold,
            "interval": self.interval,
            "enable_sound": self.enable_sound,
            "center_zero": self.center_zero
        }
    
    def get_dexscreener_price(self, token_address):
        return get_dexscreener_price_with_retry(token_address, self.coin_name)

    def get_mexc_futures_price(self, symbol):
        prices = self.mexc_price_fetcher.get_prices()
        return prices.get(symbol) if prices else None
    
    def get_gate_futures_price(self, symbol):
        prices = self.gate_price_fetcher.get_prices()
        return prices.get(symbol) if prices else None
    
    def start_monitoring(self):
        if self.is_running:
            return
        
        self.is_running = True
        self.thread = threading.Thread(target=self.monitoring_function)
        self.thread.daemon = True
        self.thread.start()
    
    def stop_monitoring(self):
        self.is_running = False
        if self.thread:
            self.thread.join(1)
            self.thread = None
    
    def monitoring_function(self):
        time.sleep(random.uniform(0, 2.0))
        
        while self.is_running:
            try:
                dex_price, dex_name = self.get_dexscreener_price(self.contract)
                
                if dex_price is not None:
                    mexc_price = self.get_mexc_futures_price(self.symbol)
                    gate_price = self.get_gate_futures_price(self.symbol)
                    
                    mexc_diff = None
                    mexc_percent = None
                    gate_diff = None
                    gate_percent = None
                    
                    if mexc_price is not None:
                        mexc_diff = mexc_price - dex_price
                        mexc_percent = (mexc_diff / dex_price) * 100
                        self.last_mexc_percent = mexc_percent
                        if self.enable_sound and mexc_percent is not None:
                            if ((self.threshold >= 0 and mexc_percent >= self.threshold) or
                                (self.threshold < 0 and mexc_percent <= self.threshold)):
                                PriceMonitor.send_notification(f"Спред {self.coin_name} (MEXC) {mexc_percent:.2f}% {('превысил порог ' + str(abs(self.threshold)) + '%') if self.threshold >= 0 else ('ниже порога ' + str(self.threshold) + '%')}")
                    
                    if gate_price is not None:
                        gate_diff = gate_price - dex_price
                        gate_percent = (gate_diff / dex_price) * 100
                        self.last_gate_percent = gate_percent
                        if self.enable_sound and gate_percent is not None:
                            if ((self.threshold >= 0 and gate_percent >= self.threshold) or
                                (self.threshold < 0 and gate_percent <= self.threshold)):
                                PriceMonitor.send_notification(f"Спред {self.coin_name} (Gate) {gate_percent:.2f}% {('превысил порог ' + str(abs(self.threshold)) + '%') if self.threshold >= 0 else ('ниже порога ' + str(self.threshold) + '%')}")
                    
                    current_time = datetime.now().strftime("%H:%M:%S")
                    self.last_update_time = current_time
                    
                    self.dex_prices.append(dex_price)
                    self.mexc_prices.append(mexc_price if mexc_price is not None else float('nan'))
                    self.gate_prices.append(gate_price if gate_price is not None else float('nan'))
                    self.mexc_spreads.append(mexc_percent if mexc_percent is not None else float('nan'))
                    self.gate_spreads.append(gate_percent if gate_percent is not None else float('nan'))
                    self.timestamps.append(datetime.now().timestamp())
                    
                    self.data_updated.emit({
                        "dex_price": dex_price,
                        "mexc_price": mexc_price,
                        "gate_price": gate_price,
                        "mexc_diff": mexc_diff,
                        "mexc_percent": mexc_percent,
                        "gate_diff": gate_diff,
                        "gate_percent": gate_percent,
                        "time": current_time,
                        "dex_name": dex_name
                    })
                    
                    self.spread_changed.emit(mexc_percent if mexc_percent is not None else 0.0, 
                                           gate_percent if gate_percent is not None else 0.0, 
                                           self.symbol)
                
                actual_interval = max(self.interval, 1.0)
                jitter = random.uniform(-0.1, 0.1) * actual_interval
                time.sleep(max(0.2, actual_interval + jitter))
                
            except Exception as e:
                print(f"[{self.coin_name}] Ошибка в мониторинге: {str(e)}")
                time.sleep(self.interval)
    
    def get_display_data(self, max_points=500):
        if not self.timestamps:
            return {
                "timestamps": [],
                "dex_prices": [],
                "mexc_prices": [],
                "gate_prices": [],
                "mexc_spreads": [],
                "gate_spreads": [],
                "total_points": 0,
                "display_points": 0
            }
            
        if len(self.dex_prices) > max_points:
            display_indices = list(range(len(self.dex_prices) - max_points, len(self.dex_prices)))
            display_timestamps = self.timestamps[-max_points:]
            display_dex = self.dex_prices[-max_points:]
            display_mexc = self.mexc_prices[-max_points:]
            display_gate = self.gate_prices[-max_points:]
            display_mexc_spreads = self.mexc_spreads[-max_points:]
            display_gate_spreads = self.gate_spreads[-max_points:]
        else:
            display_indices = list(range(len(self.dex_prices)))
            display_timestamps = self.timestamps
            display_dex = self.dex_prices
            display_mexc = self.mexc_prices
            display_gate = self.gate_prices
            display_mexc_spreads = self.mexc_spreads
            display_gate_spreads = self.gate_spreads
        
        return {
            "timestamps": display_timestamps,
            "dex_prices": display_dex,
            "mexc_prices": display_mexc,
            "gate_prices": display_gate,
            "mexc_spreads": display_mexc_spreads,
            "gate_spreads": display_gate_spreads,
            "total_points": len(self.dex_prices),
            "display_points": len(display_dex)
        }

class MEXCPriceFetcher:
    """Класс для периодического получения цен всех фьючерсов MEXC"""
    def __init__(self, interval=2.0):
        self.interval = interval
        self.prices = {}
        self.is_running = False
        self.thread = None
        self.lock = threading.Lock()

    def start(self):
        if self.is_running:
            return
        self.is_running = True
        self.thread = threading.Thread(target=self.fetch_loop)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.is_running = False
        if self.thread:
            self.thread.join(1)
            self.thread = None

    def fetch_loop(self):
        while self.is_running:
            prices = get_mexc_futures_prices_with_retry()
            with self.lock:
                self.prices = prices if prices is not None else {}
            time.sleep(self.interval)

    def get_prices(self):
        with self.lock:
            return self.prices.copy()

class GatePriceFetcher:
    """Класс для периодического получения цен всех фьючерсов Gate.io"""
    def __init__(self, interval=2.0):
        self.interval = interval
        self.prices = {}
        self.is_running = False
        self.thread = None
        self.lock = threading.Lock()

    def start(self):
        if self.is_running:
            return
        self.is_running = True
        self.thread = threading.Thread(target=self.fetch_loop)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.is_running = False
        if self.thread:
            self.thread.join(1)
            self.thread = None

    def fetch_loop(self):
        while self.is_running:
            prices = get_gate_futures_prices_with_retry()
            with self.lock:
                self.prices = prices if prices is not None else {}
            time.sleep(self.interval)

    def get_prices(self):
        with self.lock:
            return self.prices.copy()

class PriceMonitor(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Монитор спреда криптовалют")
        self.setMinimumSize(1200, 700)
        
        self.coin_monitors = {}
        self.current_coin = None
        self.block_auto_selection = False
        
        # Инициализация Price Fetchers
        self.mexc_price_fetcher = MEXCPriceFetcher(interval=2.0)
        self.mexc_price_fetcher.start()
        self.gate_price_fetcher = GatePriceFetcher(interval=2.0)
        self.gate_price_fetcher.start()
        
        self.setup_sound()
        self.init_ui()
        self.load_config()
    
    def setup_sound(self):
        try:
            self.os_type = platform.system()
            if self.os_type == "Darwin":
                self.sound_cmd = ["afplay", "/System/Library/Sounds/Ping.aiff"]
                print(f"Настроено воспроизведение звука через системные команды для macOS")
            elif self.os_type == "Windows":
                if USE_SOUND_EFFECT is True:
                    self.alert_sound = QSoundEffect()
                    self.alert_sound.setSource(QUrl.fromLocalFile("beep.wav"))
                    self.alert_sound.setVolume(0.5)
                    print("Инициализирован QSoundEffect")
                elif USE_SOUND_EFFECT is False:
                    self.alert_sound = QSound("beep.wav")
                    print("Инициализирован QSound")
                else:
                    self.alert_sound = None
                    print("Звуковая система недоступна - звуковые сигналы отключены")
            elif self.os_type == "Linux":
                self.sound_cmd = ["paplay", "/usr/share/sounds/freedesktop/stereo/bell.oga"]
                print(f"Настроено воспроизведение звука через системные команды для Linux")
            else:
                self.sound_cmd = None
                self.alert_sound = None
                print(f"Неизвестная ОС: {self.os_type} - звуковые сигналы отключены")
        except Exception as e:
            print(f"Ошибка при инициализации звука: {e}")
            self.sound_cmd = None
            self.alert_sound = None
    
    @staticmethod
    def send_notification(message):
        try:
            app = QApplication.instance()
            main_window = None
            for widget in app.topLevelWidgets():
                if isinstance(widget, PriceMonitor):
                    main_window = widget
                    break
                    
            if main_window:
                if hasattr(main_window, 'sound_cmd') and main_window.sound_cmd is not None:
                    try:
                        subprocess.run(main_window.sound_cmd, timeout=2, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
                    except subprocess.TimeoutExpired:
                        print("Воспроизведение звука заняло слишком много времени")
                elif hasattr(main_window, 'alert_sound') and main_window.alert_sound is not None:
                    if USE_SOUND_EFFECT is True:
                        main_window.alert_sound.play()
                    elif USE_SOUND_EFFECT is False:
                        main_window.alert_sound.play()
            print(f"Звуковой сигнал: {message}")
        except Exception as e:
            print(f"Ошибка при проигрывании звукового сигнала: {e}")
    
    def load_config(self):
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                
                for coin_data in config.get('coins', []):
                    try:
                        self.add_coin(coin_data, start_monitoring=False)
                    except Exception as e:
                        print(f"Ошибка при загрузке монеты {coin_data.get('symbol')}: {e}")
                
                print(f"Загружено {len(config.get('coins', []))} монет из конфигурации")
                
                if config.get('auto_start', True) and self.coin_monitors:
                    self.start_all_coins()
        except Exception as e:
            print(f"Ошибка при загрузке конфигурации: {e}")
    
    def save_config(self):
        try:
            config = {
                'coins': [monitor.to_dict() for monitor in self.coin_monitors.values()],
                'auto_start': True
            }
            
            with open(CONFIG_FILE, 'w') as f:
                json.dump(config, f, indent=2)
            
            print(f"Сохранено {len(config['coins'])} монет в конфигурацию")
        except Exception as e:
            print(f"Ошибка при сохранении конфигурации: {e}")
            
    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)
        splitter = QSplitter(Qt.Horizontal)
        splitter.setChildrenCollapsible(False)
        
        list_widget = QWidget()
        list_layout = QVBoxLayout(list_widget)
        
        self.coins_table = QTableWidget(0, 3)
        self.coins_table.setHorizontalHeaderLabels(["Монета", "MEXC", "Gate"])
        self.coins_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.coins_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.coins_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.coins_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.coins_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.coins_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.coins_table.setAlternatingRowColors(True)
        self.coins_table.setColumnWidth(1, 80)
        self.coins_table.setColumnWidth(2, 80)
        self.coins_table.verticalHeader().setVisible(False)
        self.coins_table.setSortingEnabled(False)
        self.coins_table.selectionModel().selectionChanged.connect(self.on_coin_selection_changed)
        
        buttons_layout = QHBoxLayout()
        self.add_button = QPushButton("Добавить монету")
        self.add_button.clicked.connect(self.show_add_coin_dialog)
        self.remove_button = QPushButton("Удалить монету")
        self.remove_button.clicked.connect(self.remove_selected_coin)
        self.remove_button.setEnabled(False)
        buttons_layout.addWidget(self.add_button)
        buttons_layout.addWidget(self.remove_button)
        
        list_layout.addWidget(QLabel("<b>Список отслеживаемых монет</b>"))
        list_layout.addWidget(self.coins_table)
        list_layout.addLayout(buttons_layout)
        
        detail_widget = QWidget()
        self.detail_layout = QVBoxLayout(detail_widget)
        
        self.detail_header = QLabel("<h3>Выберите монету слева или добавьте новую</h3>")
        self.detail_header.setAlignment(Qt.AlignCenter)
        self.detail_layout.addWidget(self.detail_header)
        
        self.data_section = QGroupBox("Текущие данные")
        data_layout = QGridLayout(self.data_section)
        
        self.dex_price_label = QLabel("DEX: --")
        self.mexc_price_label = QLabel("MEXC: --")
        self.gate_price_label = QLabel("Gate: --")
        self.last_update_label = QLabel("Последнее обновление: --")
        
        font = QFont()
        font.setBold(True)
        font.setPointSize(12)
        self.dex_price_label.setFont(font)
        self.mexc_price_label.setFont(font)
        self.gate_price_label.setFont(font)
        
        data_layout.addWidget(self.dex_price_label, 0, 0)
        data_layout.addWidget(self.mexc_price_label, 0, 1)
        data_layout.addWidget(self.gate_price_label, 0, 2)
        data_layout.addWidget(self.last_update_label, 1, 0, 1, 3)
        
        settings_layout = QHBoxLayout()
        settings_layout.addWidget(QLabel("Порог спреда (%):"))
        
        self.threshold_spinbox = QDoubleSpinBox()
        self.threshold_spinbox.setRange(-100, 100)
        self.threshold_spinbox.setSingleStep(0.1)
        self.threshold_spinbox.setDecimals(2)
        self.threshold_spinbox.valueChanged.connect(self.on_threshold_spinbox_changed)
        settings_layout.addWidget(self.threshold_spinbox)
        
        self.sound_checkbox = QCheckBox("Звуковой сигнал")
        self.sound_checkbox.stateChanged.connect(self.on_sound_checkbox_changed)
        settings_layout.addWidget(self.sound_checkbox)
        
        data_layout.addLayout(settings_layout, 2, 0, 1, 3)
        
        self.detail_layout.addWidget(self.data_section)
        
        self.chart_section = QGroupBox("Графики")
        chart_layout = QVBoxLayout(self.chart_section)
        
        self.price_plot_widget = pg.PlotWidget()
        self.price_plot_widget.setBackground('w')
        self.price_plot_widget.showGrid(x=True, y=True)
        self.price_plot_widget.setLabel('left', 'Цена (USDT)')
        self.price_plot_widget.setLabel('bottom', 'Время')
        self.price_plot_widget.addLegend()
        self.price_plot_widget.setMouseEnabled(x=True, y=True)
        self.price_plot_widget.enableAutoRange()
        
        self.dex_curve = self.price_plot_widget.plot(pen=pg.mkPen(color=(0, 150, 0), width=4), name='DEX')
        self.mexc_curve = self.price_plot_widget.plot(pen=pg.mkPen(color=(0, 0, 200), width=4), name='MEXC')
        self.gate_curve = self.price_plot_widget.plot(pen=pg.mkPen(color=(200, 0, 200), width=4), name='Gate')
        
        self.spread_plot_widget = pg.PlotWidget()
        self.spread_plot_widget.setBackground('w')
        self.spread_plot_widget.showGrid(x=True, y=True)
        self.spread_plot_widget.setLabel('left', 'Спред (%)')
        self.spread_plot_widget.setLabel('bottom', 'Время')
        self.spread_plot_widget.addLegend()
        self.spread_plot_widget.setMouseEnabled(x=True, y=True)
        self.spread_plot_widget.enableAutoRange()
        
        self.price_plot_widget.plotItem.setAxisItems({'bottom': pg.DateAxisItem()})
        self.spread_plot_widget.plotItem.setAxisItems({'bottom': pg.DateAxisItem()})
        
        self.zero_line = pg.InfiniteLine(pos=0, angle=0, pen=pg.mkPen(color=(100, 100, 100), width=2, style=Qt.DashLine))
        self.spread_plot_widget.addItem(self.zero_line)
        
        self.threshold_line = pg.InfiniteLine(pos=2.0, angle=0, pen=pg.mkPen(color=(255, 0, 0), width=2, style=Qt.DashLine))
        self.spread_plot_widget.addItem(self.threshold_line)
        
        self.mexc_spread_curve = self.spread_plot_widget.plot(pen=pg.mkPen(color=(0, 0, 200), width=4), name='Спред MEXC')
        self.gate_spread_curve = self.spread_plot_widget.plot(pen=pg.mkPen(color=(200, 0, 200), width=4), name='Спред Gate')
        
        chart_layout.addWidget(self.price_plot_widget)
        chart_layout.addWidget(self.spread_plot_widget)
        
        history_layout = QHBoxLayout()
        history_layout.addWidget(QLabel("Отображать точек истории:"))
        self.history_points = QSlider(Qt.Horizontal)
        self.history_points.setMinimum(50)
        self.history_points.setMaximum(10000)
        self.history_points.setValue(500)
        self.history_points.setTickPosition(QSlider.TicksBelow)
        self.history_points.setTickInterval(500)
        self.history_points.valueChanged.connect(self.update_charts)
        
        self.history_label = QLabel(str(self.history_points.value()))
        self.history_points.valueChanged.connect(lambda value: self.history_label.setText(str(value)))
        
        history_layout.addWidget(self.history_points)
        history_layout.addWidget(self.history_label)
        chart_layout.addLayout(history_layout)
        
        self.detail_layout.addWidget(self.chart_section)
        
        control_layout = QHBoxLayout()
        self.start_button = QPushButton("Старт")
        self.start_button.clicked.connect(self.start_selected_coin)
        self.start_button.setEnabled(False)
        
        self.stop_button = QPushButton("Стоп")
        self.stop_button.clicked.connect(self.stop_selected_coin)
        self.stop_button.setEnabled(False)
        
        self.clear_button = QPushButton("Очистить график")
        self.clear_button.clicked.connect(self.clear_selected_coin)
        self.clear_button.setEnabled(False)
        
        self.copy_contract_button = QPushButton("Копировать контракт")
        self.copy_contract_button.clicked.connect(self.copy_contract)
        self.copy_contract_button.setEnabled(False)
        
        self.start_all_button = QPushButton("Старт все")
        self.start_all_button.clicked.connect(self.start_all_coins)
        self.start_all_button.setEnabled(False)
        
        self.stop_all_button = QPushButton("Стоп все")
        self.stop_all_button.clicked.connect(self.stop_all_coins)
        self.stop_all_button.setEnabled(False)
        
        control_layout.addWidget(self.start_button)
        control_layout.addWidget(self.stop_button)
        control_layout.addWidget(self.clear_button)
        control_layout.addWidget(self.copy_contract_button)
        control_layout.addWidget(self.start_all_button)
        control_layout.addWidget(self.stop_all_button)
        
        self.detail_layout.addLayout(control_layout)
        
        self.data_section.setVisible(False)
        self.chart_section.setVisible(False)
        
        splitter.addWidget(list_widget)
        splitter.addWidget(detail_widget)
        splitter.setSizes([300, 900])
        main_layout.addWidget(splitter)
        
        self.update_timer = QTimer(self)
        self.update_timer.timeout.connect(self.update_coins_table)
        self.update_timer.start(1000)
        
    def show_add_coin_dialog(self):
        dialog = AddCoinDialog(self)
        if dialog.exec_() == QDialog.Accepted:
            coin_data = dialog.get_data()
            self.add_coin(coin_data)
    
    def add_coin(self, coin_data, start_monitoring=True):
        symbol = coin_data["symbol"]
        if symbol in self.coin_monitors:
            QMessageBox.warning(self, "Ошибка", f"Монета {symbol} уже добавлена")
            return
        
        monitor = CoinMonitor(coin_data, self.mexc_price_fetcher, self.gate_price_fetcher)
        monitor.data_updated.connect(lambda data: self.update_coin_data(symbol, data))
        monitor.spread_changed.connect(self.on_spread_changed)
        
        self.coin_monitors[symbol] = monitor
        self.update_coins_table()
        self.start_all_button.setEnabled(True)
        self.select_coin(symbol)
        
        if start_monitoring:
            monitor.start_monitoring()
            
        self.save_config()
        self.update_control_buttons()
    
    def remove_selected_coin(self):
        if not self.current_coin:
            return
        
        if self.current_coin in self.coin_monitors:
            self.coin_monitors[self.current_coin].stop_monitoring()
            del self.coin_monitors[self.current_coin]
            
            self.update_coins_table()
            self.current_coin = None
            self.detail_header.setText("<h3>Выберите монету слева или добавьте новую</h3>")
            self.data_section.setVisible(False)
            self.chart_section.setVisible(False)
            
            self.remove_button.setEnabled(False)
            self.start_button.setEnabled(False)
            self.stop_button.setEnabled(False)
            self.clear_button.setEnabled(False)
            self.copy_contract_button.setEnabled(False)
            
            if not self.coin_monitors:
                self.start_all_button.setEnabled(False)
                self.stop_all_button.setEnabled(False)
                
            self.save_config()
    
    def on_coin_selection_changed(self, selected, deselected):
        if self.block_auto_selection:
            return
            
        if not selected.indexes():
            self.current_coin = None
            self.detail_header.setText("<h3>Выберите монету слева или добавьте новую</h3>")
            self.data_section.setVisible(False)
            self.chart_section.setVisible(False)
            self.remove_button.setEnabled(False)
            self.start_button.setEnabled(False)
            self.stop_button.setEnabled(False)
            self.clear_button.setEnabled(False)
            self.copy_contract_button.setEnabled(False)
            return
            
        row = selected.indexes()[0].row()
        symbol = self.coins_table.item(row, 0).data(Qt.UserRole)
        self.current_coin = symbol
        
        monitor = self.coin_monitors.get(symbol)
        if monitor:
            coin_name = monitor.coin_name
            self.detail_header.setText(f"<h3>Монета: {coin_name} ({symbol})</h3>")
            self.data_section.setVisible(True)
            self.chart_section.setVisible(True)
            self.remove_button.setEnabled(True)
            
            self.threshold_spinbox.blockSignals(True)
            self.threshold_spinbox.setValue(monitor.threshold)
            self.threshold_spinbox.blockSignals(False)
            
            self.sound_checkbox.blockSignals(True)
            self.sound_checkbox.setChecked(monitor.enable_sound)
            self.sound_checkbox.blockSignals(False)
            
            self.threshold_line.setValue(monitor.threshold)
            self.update_charts()
            self.update_control_buttons()
    
    def on_threshold_spinbox_changed(self, value):
        if not self.current_coin or self.current_coin not in self.coin_monitors:
            return
            
        monitor = self.coin_monitors[self.current_coin]
        monitor.threshold = value
        self.threshold_line.setValue(value)
        self.update_coins_table()
        self.save_config()
        print(f"Порог для {self.current_coin} изменен на {value}%")
    
    def on_sound_checkbox_changed(self, state):
        if not self.current_coin or self.current_coin not in self.coin_monitors:
            return
            
        monitor = self.coin_monitors[self.current_coin]
        monitor.enable_sound = (state == Qt.Checked)
        self.save_config()
        print(f"Звуковой сигнал для {self.current_coin} {'включен' if monitor.enable_sound else 'выключен'}")
    
    def select_coin(self, symbol):
        for row in range(self.coins_table.rowCount()):
            if self.coins_table.item(row, 0).data(Qt.UserRole) == symbol:
                self.coins_table.selectRow(row)
                break
    
    def copy_contract(self):
        if not self.current_coin or self.current_coin not in self.coin_monitors:
            return
            
        contract = self.coin_monitors[self.current_coin].contract
        clipboard = QApplication.clipboard()
        clipboard.setText(contract)
        QMessageBox.information(self, "Успех", "Адрес контракта скопирован в буфер обмена")
    
    def update_coins_table(self):
        current_symbol = self.current_coin
        self.block_auto_selection = True
        
        selected_rows = self.coins_table.selectionModel().selectedRows()
        selected_symbol = None
        if selected_rows:
            row = selected_rows[0].row()
            item = self.coins_table.item(row, 0)
            if item:
                selected_symbol = item.data(Qt.UserRole)
        
        self.coins_table.setRowCount(0)
        
        sorted_monitors = sorted(
            self.coin_monitors.items(), 
            key=lambda x: max(x[1].last_mexc_percent or float('-inf'), x[1].last_gate_percent or float('-inf')),
            reverse=True
        )
        
        for i, (symbol, monitor) in enumerate(sorted_monitors):
            self.coins_table.insertRow(i)
            name_item = QTableWidgetItem(monitor.coin_name)
            name_item.setData(Qt.UserRole, symbol)
            self.coins_table.setItem(i, 0, name_item)
            
            mexc_spread_value = monitor.last_mexc_percent
            mexc_spread_str = f"{mexc_spread_value:.2f}%" if mexc_spread_value is not None else "--"
            mexc_spread_item = QTableWidgetItem(mexc_spread_str)
            
            gate_spread_value = monitor.last_gate_percent
            gate_spread_str = f"{gate_spread_value:.2f}%" if gate_spread_value is not None else "--"
            gate_spread_item = QTableWidgetItem(gate_spread_str)
            
            if monitor.last_update_time:
                if mexc_spread_value is not None:
                    if mexc_spread_value > 0:
                        mexc_spread_item.setForeground(QBrush(QColor(0, 150, 0)))
                    elif mexc_spread_value < 0:
                        mexc_spread_item.setForeground(QBrush(QColor(200, 0, 0)))
                
                if gate_spread_value is not None:
                    if gate_spread_value > 0:
                        gate_spread_item.setForeground(QBrush(QColor(0, 150, 0)))
                    elif gate_spread_value < 0:
                        gate_spread_item.setForeground(QBrush(QColor(200, 0, 0)))
                
                if ((mexc_spread_value is not None and 
                     ((monitor.threshold >= 0 and mexc_spread_value >= monitor.threshold) or
                      (monitor.threshold < 0 and mexc_spread_value <= monitor.threshold))) or
                    (gate_spread_value is not None and 
                     ((monitor.threshold >= 0 and gate_spread_value >= monitor.threshold) or
                      (monitor.threshold < 0 and gate_spread_value <= monitor.threshold)))):
                    bg_color = QColor(180, 0, 0)
                    fg_color = QColor(255, 255, 255)
                    for col in range(self.coins_table.columnCount()):
                        item = self.coins_table.item(i, col) or QTableWidgetItem()
                        item.setBackground(QBrush(bg_color))
                        item.setForeground(QBrush(fg_color))
                        if col != 0:
                            self.coins_table.setItem(i, col, item)
                    name_item.setForeground(QBrush(fg_color))
            
            self.coins_table.setItem(i, 1, mexc_spread_item)
            self.coins_table.setItem(i, 2, gate_spread_item)
            
            if not monitor.is_running and not (
                (mexc_spread_value is not None and 
                 ((monitor.threshold >= 0 and mexc_spread_value >= monitor.threshold) or
                  (monitor.threshold < 0 and mexc_spread_value <= monitor.threshold))) or
                (gate_spread_value is not None and 
                 ((monitor.threshold >= 0 and gate_spread_value >= monitor.threshold) or
                  (monitor.threshold < 0 and gate_spread_value <= monitor.threshold)))
            ):
                name_item.setForeground(QBrush(QColor(150, 150, 150)))
            
        if selected_symbol:
            self.select_coin(selected_symbol)
        
        has_running = any(monitor.is_running for monitor in self.coin_monitors.values())
        has_stopped = any(not monitor.is_running for monitor in self.coin_monitors.values())
        self.start_all_button.setEnabled(has_stopped and bool(self.coin_monitors))
        self.stop_all_button.setEnabled(has_running)
        
        self.block_auto_selection = False
    
    def update_coin_data(self, symbol, data):
        if symbol != self.current_coin:
            return
            
        self.dex_price_label.setText(f"DEX: {data['dex_price']:.6f} USDT")
        self.mexc_price_label.setText(f"MEXC: {data['mexc_price']:.6f} USDT" if data['mexc_price'] is not None else "MEXC: --")
        self.gate_price_label.setText(f"Gate: {data['gate_price']:.6f} USDT" if data['gate_price'] is not None else "Gate: --")
        self.last_update_label.setText(f"Последнее обновление: {data['time']}")
        self.update_charts()
    
    def on_spread_changed(self, mexc_percent, gate_percent, symbol):
        pass
    
    def update_charts(self):
        if not self.current_coin or self.current_coin not in self.coin_monitors:
            return
            
        monitor = self.coin_monitors[self.current_coin]
        max_points = self.history_points.value()
        display_data = monitor.get_display_data(max_points)
        
        self.dex_curve.setData(display_data["timestamps"], display_data["dex_prices"])
        self.mexc_curve.setData(display_data["timestamps"], display_data["mexc_prices"])
        self.gate_curve.setData(display_data["timestamps"], display_data["gate_prices"])
        self.mexc_spread_curve.setData(display_data["timestamps"], display_data["mexc_spreads"])
        self.gate_spread_curve.setData(display_data["timestamps"], display_data["gate_spreads"])
        
        count_text = f"Всего точек: {display_data['total_points']}, Отображается: {display_data['display_points']}"
        self.history_label.setText(count_text)
        
        if monitor.center_zero and (display_data["mexc_spreads"] or display_data["gate_spreads"]):
            valid_mexc_spreads = [x for x in display_data["mexc_spreads"] if not np.isnan(x)]
            valid_gate_spreads = [x for x in display_data["gate_spreads"] if not np.isnan(x)]
            all_spreads = valid_mexc_spreads + valid_gate_spreads
            max_spread = max(abs(max(all_spreads, default=0)), abs(min(all_spreads, default=0))) if all_spreads else 1
            padding = max_spread * 0.1
            self.spread_plot_widget.setYRange(-max_spread - padding, max_spread + padding)
    
    def update_control_buttons(self):
        if not self.current_coin or self.current_coin not in self.coin_monitors:
            self.start_button.setEnabled(False)
            self.stop_button.setEnabled(False)
            self.clear_button.setEnabled(False)
            self.copy_contract_button.setEnabled(False)
            return
            
        monitor = self.coin_monitors[self.current_coin]
        self.start_button.setEnabled(not monitor.is_running)
        self.stop_button.setEnabled(monitor.is_running)
        self.clear_button.setEnabled(True)
        self.copy_contract_button.setEnabled(True)
    
    def start_selected_coin(self):
        if not self.current_coin or self.current_coin not in self.coin_monitors:
            return
            
        self.coin_monitors[self.current_coin].start_monitoring()
        self.update_control_buttons()
    
    def stop_selected_coin(self):
        if not self.current_coin or self.current_coin not in self.coin_monitors:
            return
            
        self.coin_monitors[self.current_coin].stop_monitoring()
        self.update_control_buttons()
    
    def clear_selected_coin(self):
        if not self.current_coin or self.current_coin not in self.coin_monitors:
            return
            
        monitor = self.coin_monitors[self.current_coin]
        monitor.dex_prices = []
        monitor.mexc_prices = []
        monitor.gate_prices = []
        monitor.mexc_spreads = []
        monitor.gate_spreads = []
        monitor.timestamps = []
        monitor.last_update_time = ""
        monitor.last_mexc_percent = 0.0
        monitor.last_gate_percent = 0.0
        
        self.dex_price_label.setText("DEX: --")
        self.mexc_price_label.setText("MEXC: --")
        self.gate_price_label.setText("Gate: --")
        self.last_update_label.setText("Последнее обновление: --")
        
        self.dex_curve.setData([], [])
        self.mexc_curve.setData([], [])
        self.gate_curve.setData([], [])
        self.mexc_spread_curve.setData([], [])
        self.gate_spread_curve.setData([], [])
        
        self.history_label.setText("Всего точек: 0, Отображается: 0")
        self.update_coins_table()
    
    def start_all_coins(self):
        for symbol, monitor in self.coin_monitors.items():
            monitor.start_monitoring()
        self.update_control_buttons()
    
    def stop_all_coins(self):
        for symbol, monitor in self.coin_monitors.items():
            monitor.stop_monitoring()
        self.update_control_buttons()
    
    def closeEvent(self, event):
        self.mexc_price_fetcher.stop()
        self.gate_price_fetcher.stop()
        for monitor in self.coin_monitors.values():
            monitor.stop_monitoring()
        self.update_timer.stop()
        self.save_config()
        event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = PriceMonitor()
    window.show()
    sys.exit(app.exec_())