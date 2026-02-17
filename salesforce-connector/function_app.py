import azure.functions as func
import logging
import os
import json
import csv
import io
import time
import socket
import threading
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

# Import external packages at MODULE LEVEL
import requests
from requests.adapters import HTTPAdapter
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.data.tables import TableClient

# Configuration from environment variables
KEY_VAULT_NAME = os.environ.get('KEY_VAULT_NAME')
DCE_ENDPOINT = os.environ.get('DCE_ENDPOINT')
STREAM_NAME = os.environ.get('STREAM_NAME')
ENVIRONMENTS_JSON = os.environ.get('ENVIRONMENTS_JSON')
STATE_STORAGE_ACCOUNT = os.environ.get('STATE_STORAGE_ACCOUNT')

# DNS and Connection Pool Configuration
DNS_CACHE_REFRESH_INTERVAL = int(os.environ.get('DNS_CACHE_REFRESH_INTERVAL', '25'))
BATCH_DELAY_MS = int(os.environ.get('BATCH_DELAY_MS', '400'))

# Initialize credential at MODULE LEVEL for reuse
credential = DefaultAzureCredential()

# Token cache at module level to reduce token refresh overhead
_token_cache = {'token': None, 'expiry': 0}

# CRITICAL: Request serialization lock to prevent concurrent DNS lookups
_send_lock = threading.Lock()

def get_cached_token():
    """Get cached Azure Monitor token, refresh if needed"""
    global _token_cache
    now = time.time()
    
    # Refresh token if expired or about to expire (5 min buffer)
    if _token_cache['token'] is None or now >= (_token_cache['expiry'] - 300):
        token = credential.get_token("https://monitor.azure.com/.default")
        _token_cache['token'] = token.token
        _token_cache['expiry'] = token.expires_on
        logging.info(f"🔑 Token refreshed, expires at {datetime.fromtimestamp(token.expires_on).strftime('%H:%M:%S')}")
    
    return _token_cache['token']

# ============================================================================
# DNS CACHE MANAGER (Global for all endpoints)
# ============================================================================

class DNSCacheManager:
    """Global DNS cache manager for all network endpoints"""
    
    def __init__(self):
        self.cache = {}  # hostname -> (ip, timestamp)
        self.cache_ttl = 300  # 5 minutes TTL
    
    def resolve(self, hostname: str, force_refresh: bool = False) -> Optional[str]:
        """Resolve hostname to IP with caching"""
        now = time.time()
        
        # Check cache
        if not force_refresh and hostname in self.cache:
            ip, timestamp = self.cache[hostname]
            if (now - timestamp) < self.cache_ttl:
                return ip
        
        # Resolve DNS
        try:
            ip = socket.gethostbyname(hostname)
            self.cache[hostname] = (ip, now)
            return ip
        except socket.gaierror as e:
            logging.warning(f"⚠️ DNS resolution failed for {hostname}: {e}")
            return None
    
    def refresh(self, hostname: str):
        """Force refresh DNS for a specific hostname"""
        return self.resolve(hostname, force_refresh=True)
    
    def clear(self):
        """Clear entire DNS cache"""
        self.cache.clear()

# Global DNS cache instance
dns_cache = DNSCacheManager()

# ============================================================================
# STATE MANAGER CLASS
# ============================================================================

class StateManager:
    """Manages processing state to prevent duplicates and recover missed logs"""

    def __init__(self, storage_account_name: str, credential):
        # Pre-resolve Table Storage hostname
        table_hostname = f"{storage_account_name}.table.core.windows.net"
        dns_cache.resolve(table_hostname)
        
        self.table_client = TableClient(
            endpoint=f"https://{table_hostname}",
            table_name="SalesforceEventLogState",
            credential=credential
        )
        # Ensure table exists
        try:
            self.table_client.create_table()
            logging.info("State table created or already exists")
        except Exception as e:
            logging.debug(f"Table creation info: {str(e)}")

    def get_last_processed(self, env_name: str, event_type: str, log_date: str) -> int:
        """Get last processed sequence number for a specific log file"""
        try:
            partition_key = f"{env_name}_{event_type}"
            row_key = log_date
            
            entity = self.table_client.get_entity(
                partition_key=partition_key,
                row_key=row_key
            )
            sequence = entity.get('LastSequence', 0)
            logging.debug(f"Last processed: {partition_key}/{row_key} = Seq {sequence}")
            return sequence
        except Exception:
            # Entity doesn't exist - first time processing
            logging.debug(f"No state found for {env_name}/{event_type}/{log_date} (first time)")
            return 0

    def update_last_processed(self, env_name: str, event_type: str, 
                            log_date: str, sequence: int, log_file_id: str) -> None:
        """Update last processed sequence number"""
        try:
            entity = {
                'PartitionKey': f"{env_name}_{event_type}",
                'RowKey': log_date,
                'LastSequence': sequence,
                'LogFileId': log_file_id,
                'ProcessedAt': datetime.utcnow().isoformat()
            }
            self.table_client.upsert_entity(entity)
            logging.info(f"State updated: {env_name}/{event_type}/{log_date} → Seq {sequence}")
        except Exception as e:
            logging.error(f"Failed to update state: {str(e)}")
            # Don't raise - state update failure shouldn't stop ingestion

# ============================================================================
# SALESFORCE PROCESSOR CLASS
# ============================================================================

class SalesforceProcessor:
    """Handles Salesforce event log retrieval and processing"""

    def __init__(self, env_config: Dict[str, str], credential, kv_client: SecretClient):
        self.env_name = env_config['name']
        self.sf_domain = env_config['salesforceDomain']
        self.dcr_immutable_id = env_config.get('dcrImmutableId', '')
        self.credential = credential
        self.kv_client = kv_client
        self.access_token = None
        
        # Extract and cache hostnames
        self.sf_hostname = urlparse(self.sf_domain).hostname
        self.dce_hostname = urlparse(DCE_ENDPOINT).hostname
        
        # Pre-resolve all hostnames
        self.resolve_all_dns()

    def resolve_all_dns(self):
        """Pre-resolve all DNS for this environment"""
        
        # Resolve Salesforce
        if self.sf_hostname:
            dns_cache.resolve(self.sf_hostname)
        
        # Resolve DCE
        if self.dce_hostname:
            dns_cache.resolve(self.dce_hostname)
        

    def refresh_all_dns(self):
        """Force refresh all DNS for this environment"""
        
        if self.sf_hostname:
            dns_cache.refresh(self.sf_hostname)
        
        if self.dce_hostname:
            dns_cache.refresh(self.dce_hostname)
        

    def get_salesforce_credentials(self) -> tuple:
        """Retrieve Salesforce OAuth credentials from Key Vault"""
        try:
            client_id_secret = self.kv_client.get_secret(f"sf-{self.env_name}-clientid")
            client_secret_secret = self.kv_client.get_secret(f"sf-{self.env_name}-clientsecret")
            return client_id_secret.value, client_secret_secret.value
        except Exception as e:
            logging.error(f"Failed to retrieve credentials for {self.env_name}: {str(e)}")
            raise

    def authenticate_salesforce(self) -> str:
        """Authenticate with Salesforce and get access token"""
        try:
            client_id, client_secret = self.get_salesforce_credentials()
            token_url = f"{self.sf_domain}/services/oauth2/token"
            data = {
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret': client_secret
            }

            # Verify DNS is resolved
            dns_cache.resolve(self.sf_hostname)

            response = requests.post(token_url, data=data, timeout=30)
            response.raise_for_status()
            self.access_token = response.json()['access_token']
            
            logging.info(f"Successfully authenticated with Salesforce for {self.env_name}")
            return self.access_token
        except Exception as e:
            logging.error(f"Salesforce authentication failed for {self.env_name}: {str(e)}")
            # Try DNS refresh and retry once
            logging.info(f"Attempting DNS refresh and retry for {self.env_name}...")
            dns_cache.refresh(self.sf_hostname)
            raise

    def query_event_log_files(self, state_mgr: StateManager) -> List[Dict]:
        """Query Salesforce for NEW event log files only (deduplication applied)"""
        try:
            # Verify DNS is resolved
            dns_cache.resolve(self.sf_hostname)
            
            # Query last 3 hours to handle latency and recover missed runs
            start_time = (datetime.utcnow() - timedelta(hours=3)).strftime('%Y-%m-%dT%H:%M:%SZ')
            end_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

            query = (
                f"SELECT Id,EventType,LogDate,Interval,Sequence,CreatedDate,LogFile,LogFileLength "
                f"FROM EventLogFile "
                f"WHERE Interval='Hourly' "
                f"AND CreatedDate>={start_time} "
                f"AND CreatedDate<{end_time} "
                f"ORDER BY LogDate, Sequence"
            )

            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }

            query_url = f"{self.sf_domain}/services/data/v55.0/query"
            response = requests.get(query_url, headers=headers, params={'q': query}, timeout=60)
            response.raise_for_status()

            all_records = response.json().get('records', [])
            
            # Filter out already-processed files using state tracking
            new_records = []
            for record in all_records:
                event_type = record.get('EventType', '')
                log_date = record.get('LogDate', '')
                sequence = record.get('Sequence', 0)
                
                last_seq = state_mgr.get_last_processed(self.env_name, event_type, log_date)
                
                if sequence > last_seq:
                    new_records.append(record)
                    logging.info(f"NEW: {event_type}/{log_date}/Seq{sequence} (last was {last_seq})")
                else:
                    logging.debug(f"SKIP: {event_type}/{log_date}/Seq{sequence} (already processed)")

            logging.info(f"Found {len(new_records)} NEW log files for {self.env_name} "
                        f"(filtered from {len(all_records)} total)")
            return new_records
        except Exception as e:
            logging.error(f"Failed to query event log files for {self.env_name}: {str(e)}")
            raise

    def download_csv_file(self, log_file_path: str) -> str:
        """Download CSV log file from Salesforce with retry logic for DNS failures"""
        
        MAX_RETRIES = 5
        RETRY_BASE_DELAY = 3  # seconds
        
        for attempt in range(MAX_RETRIES):
            try:
                # Verify DNS is resolved before request
                dns_cache.resolve(self.sf_hostname)
                
                headers = {'Authorization': f'Bearer {self.access_token}'}
                url = f"{self.sf_domain}{log_file_path}"
                
                # Longer timeout for large CSV files
                response = requests.get(url, headers=headers, timeout=120)
                response.raise_for_status()
                
                file_size_kb = len(response.text) / 1024
                logging.info(f"✅ Downloaded CSV file: {log_file_path} ({file_size_kb:.0f} KB)")
                return response.text
                
            except requests.exceptions.Timeout as e:
                # Timeout errors - retry with backoff
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_BASE_DELAY * (2 ** attempt)  # Exponential backoff
                    logging.warning(
                        f"⏱️ Timeout downloading {log_file_path} for {self.env_name} "
                        f"(attempt {attempt + 1}/{MAX_RETRIES}) - Retrying in {wait_time}s"
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    logging.error(f"❌ Timeout failed after {MAX_RETRIES} attempts: {log_file_path}")
                    raise
                    
            except (requests.exceptions.ConnectionError, OSError, socket.gaierror) as e:
                # Check if DNS-related
                error_str = str(e).lower()
                is_dns_error = any(indicator in error_str for indicator in [
                    'failed to resolve', 'name resolution', 'gaierror', 'errno -3',
                    'nodename nor servname', 'temporary failure in name resolution'
                ])
                
                if is_dns_error and attempt < MAX_RETRIES - 1:
                    # DNS error - refresh cache and retry
                    wait_time = RETRY_BASE_DELAY * (2 ** attempt)  # Exponential backoff
                    logging.warning(
                        f"⚠️ DNS failure downloading {log_file_path} for {self.env_name} "
                        f"(attempt {attempt + 1}/{MAX_RETRIES}) - Retrying in {wait_time}s"
                    )
                    
                    # Force DNS refresh
                    dns_cache.refresh(self.sf_hostname)
                    time.sleep(wait_time)
                    continue
                elif not is_dns_error and attempt < MAX_RETRIES - 1:
                    # Non-DNS network error - still retry
                    wait_time = RETRY_BASE_DELAY * (2 ** attempt)
                    logging.warning(
                        f"⚠️ Network error downloading {log_file_path} for {self.env_name} "
                        f"(attempt {attempt + 1}/{MAX_RETRIES}): {str(e)[:100]} - Retrying in {wait_time}s"
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    # Max retries exceeded
                    logging.error(
                        f"❌ Failed to download {log_file_path} after {MAX_RETRIES} attempts: {str(e)}"
                    )
                    raise
                    
            except requests.exceptions.HTTPError as e:
                # HTTP errors (401, 404, 500, etc.) - don't retry authentication/authorization errors
                status_code = e.response.status_code if hasattr(e, 'response') else 0
                
                if status_code in [401, 403, 404]:
                    # Don't retry client errors - log and fail immediately
                    logging.error(
                        f"❌ HTTP {status_code} error downloading {log_file_path}: {str(e)}"
                    )
                    raise
                elif status_code >= 500 and attempt < MAX_RETRIES - 1:
                    # Retry server errors (5xx)
                    wait_time = RETRY_BASE_DELAY * (2 ** attempt)
                    logging.warning(
                        f"⚠️ HTTP {status_code} (server error) downloading {log_file_path} "
                        f"for {self.env_name} (attempt {attempt + 1}/{MAX_RETRIES}) - Retrying in {wait_time}s"
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    logging.error(f"❌ HTTP error downloading {log_file_path}: {str(e)}")
                    raise
                    
            except Exception as e:
                # Unexpected errors - log and fail
                logging.error(f"❌ Unexpected error downloading {log_file_path}: {str(e)}")
                raise
        
        # Should never reach here due to raises above, but for safety:
        raise Exception(f"Failed to download {log_file_path} after {MAX_RETRIES} attempts")

    def parse_csv_to_json(self, csv_content: str, event_type: str, log_date: str, 
                         sequence: int) -> List[Dict[str, Any]]:
        """Parse CSV content and convert to JSON array with specified schema"""
        try:
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            json_records = []

            for row in csv_reader:
                if not row or all(v == '' for v in row.values()):
                    continue

                # Extract key fields - prioritize TIMESTAMP_DERIVED for event time
                timestamp = row.get('TIMESTAMP', '')
                timestamp_derived_raw = row.get('TIMESTAMP_DERIVED', '')

                # Parse timestamp_derived - try ISO8601 first, then compact format
                if timestamp_derived_raw:
                    try:
                        # Salesforce TIMESTAMP_DERIVED: ISO8601 like "2026-02-12T06:16:52.411Z"
                        dt = datetime.strptime(timestamp_derived_raw, '%Y-%m-%dT%H:%M:%S.%fZ')
                    except ValueError:
                        try:
                            # Fallback: compact TIMESTAMP like "20260212061652.411"
                            dt = datetime.strptime(timestamp_derived_raw, '%Y%m%d%H%M%S.%f')
                        except ValueError:
                            dt = datetime.utcnow()
                else:
                    # No TIMESTAMP_DERIVED, try compact TIMESTAMP
                    try:
                        dt = datetime.strptime(timestamp, '%Y%m%d%H%M%S.%f')
                    except ValueError:
                        dt = datetime.utcnow()

                timestamp_derived = dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')

                # Build unique identifier for deduplication
                unique_id = f"{self.env_name}_{event_type}_{log_date}_{sequence}"

                # Build the record according to the specified schema
                record = {
                    'TimeGenerated': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                    'Timestamp': timestamp,
                    'TimestampDerived': timestamp_derived,
                    'EventType': event_type,
                    'EnvironmentName': self.env_name,
                    'RequestId': row.get('REQUEST_ID', ''),
                    'UserId': row.get('USER_ID', ''),
                    'UserName': row.get('USER_NAME', ''),
                    'UniqueLogFileId': unique_id,
                    'LogFileSequence': sequence,
                    'JsonData': row,
                    'RawData': ','.join([f'"{v}"' if ',' in str(v) else str(v) for v in row.values()])
                }

                json_records.append(record)

            logging.info(f"Parsed {len(json_records)} records from CSV for {self.env_name}")
            return json_records
        except Exception as e:
            logging.error(f"Failed to parse CSV: {str(e)}")
            raise

    def send_to_log_analytics(self, records: List[Dict[str, Any]]) -> None:
        """Send parsed records to Log Analytics via DCE with serialized requests and NO urllib3 retries"""
        
        if not records:
            logging.info(f"No records to send for {self.env_name}")
            return

        # Configuration for DNS-specific retries
        MAX_DNS_RETRIES = 8
        DNS_RETRY_BASE_DELAY = 3  # seconds
        MAX_BATCH_SIZE_BYTES = 900_000

        # Pre-resolve DCE DNS
        dns_cache.resolve(self.dce_hostname)

        def is_dns_error(exception) -> bool:
            """Check if exception is DNS-related"""
            error_str = str(exception).lower()
            dns_indicators = [
                'failed to resolve',
                'name resolution',
                'gaierror',
                'errno -3',
                'nodename nor servname',
                'temporary failure in name resolution',
                'nameresolutionerror'
            ]
            return any(indicator in error_str for indicator in dns_indicators)

        def send_batch_with_retry(batch_data: List[Dict[str, Any]], batch_num: int, batch_size_bytes: int) -> bool:
            """
            Send a single batch with SERIALIZED execution and NO urllib3 automatic retries.
            
            CRITICAL CHANGES:
            1. Threading lock ensures only ONE request at a time
            2. HTTPAdapter with max_retries=0 disables urllib3's automatic retry mechanism
            3. ALL retry logic handled in this function's outer loop
            """
            
            # ACQUIRE LOCK: Only one thread can send at a time
            with _send_lock:
                for attempt in range(MAX_DNS_RETRIES):
                    session = None
                    try:
                        # Use cached token (refreshed every 10 minutes)
                        token = get_cached_token()
                        
                        dce_url = f"{DCE_ENDPOINT}/dataCollectionRules/{self.dcr_immutable_id}/streams/{STREAM_NAME}?api-version=2023-01-01"
                        
                        headers = {
                            'Authorization': f'Bearer {token}',
                            'Content-Type': 'application/json',
                            'Connection': 'close'  # Force connection close
                        }
                        
                        # Verify DNS is resolved before creating session
                        dns_cache.resolve(self.dce_hostname)
                        
                        # Create fresh session with MINIMAL pool and NO automatic retries
                        session = requests.Session()
                        
                        # CRITICAL: max_retries=0 disables urllib3's automatic retry mechanism
                        adapter = HTTPAdapter(
                            max_retries=0,       # NO urllib3 retries - handle ALL retries in outer loop
                            pool_connections=1,  # Minimum pool size
                            pool_maxsize=1,      # Force single connection
                            pool_block=True      # Block if connection unavailable (enforce serialization)
                        )
                        session.mount("https://", adapter)
                        
                        # Attempt the request with explicit timeouts
                        response = session.post(
                            dce_url, 
                            headers=headers, 
                            json=batch_data, 
                            timeout=(10, 60)  # (connect timeout, read timeout)
                        )
                        response.raise_for_status()
                        
                        return True  # Success
                        
                    except requests.exceptions.Timeout as e:
                        # Timeout errors - specific handling
                        logging.warning(
                            f"⏱️ Timeout error for {self.env_name} - Batch #{batch_num} "
                            f"(attempt {attempt + 1}/{MAX_DNS_RETRIES}): {str(e)[:200]}"
                        )
                        if attempt < MAX_DNS_RETRIES - 1:
                            wait_time = DNS_RETRY_BASE_DELAY * (2 ** attempt)
                            logging.info(f"Retrying in {wait_time}s...")
                            time.sleep(wait_time)
                        else:
                            logging.error(f"❌ Timeout failed after {MAX_DNS_RETRIES} attempts for batch #{batch_num}")
                            raise
                        
                    except (requests.exceptions.ConnectionError, OSError, socket.gaierror) as e:
                        
                        if is_dns_error(e):
                            if attempt < MAX_DNS_RETRIES - 1:
                                # Exponential backoff for DNS errors
                                wait_time = DNS_RETRY_BASE_DELAY * (2 ** attempt)
                                logging.warning(
                                    f"⚠️ DNS resolution failure for {self.env_name} - Batch #{batch_num} "
                                    f"(attempt {attempt + 1}/{MAX_DNS_RETRIES}): {str(e)[:200]} "
                                    f"- Retrying in {wait_time}s"
                                )
                                
                                # Force DNS cache refresh for DCE
                                dns_cache.refresh(self.dce_hostname)
                                
                                time.sleep(wait_time)
                                
                                # Attempt to clear system DNS cache
                                try:
                                    socket.setdefaulttimeout(30)
                                except:
                                    pass
                            else:
                                # Max retries exceeded
                                logging.error(
                                    f"❌ DNS resolution failed after {MAX_DNS_RETRIES} attempts for {self.env_name} "
                                    f"- Batch #{batch_num}. Last error: {str(e)[:200]}"
                                )
                                raise
                        else:
                            # Non-DNS network error
                            logging.error(f"❌ Network error (non-DNS) for {self.env_name} - Batch #{batch_num}: {str(e)}")
                            if attempt < MAX_DNS_RETRIES - 1:
                                wait_time = DNS_RETRY_BASE_DELAY * (2 ** attempt)
                                logging.info(f"Retrying in {wait_time}s...")
                                time.sleep(wait_time)
                            else:
                                raise
                            
                    except requests.exceptions.HTTPError as e:
                        # HTTP errors (4xx, 5xx) - log and raise immediately
                        logging.error(f"❌ HTTP error sending to Log Analytics for {self.env_name} - Batch #{batch_num}: {str(e)}")
                        if hasattr(e.response, 'text'):
                            logging.error(f"Response body: {e.response.text[:500]}")
                        raise
                        
                    except Exception as e:
                        # Unexpected errors
                        logging.error(f"❌ Unexpected error sending batch #{batch_num} for {self.env_name}: {str(e)}")
                        import traceback
                        logging.error(f"Traceback: {traceback.format_exc()}")
                        raise
                    
                    finally:
                        # CRITICAL: Always close session to free connections
                        if session:
                            try:
                                session.close()
                            except Exception as close_error:
                                pass
                
                return False
            # Lock is automatically released here

        # Main batching and sending logic - STREAMING APPROACH
        try:
            current_batch = []
            current_batch_size = 0
            batch_counter = 0
            total_sent = 0
            total_records = len(records)
            start_time = time.time()
            last_progress_time = start_time

            logging.info(f"Starting to process {total_records:,} records for {self.env_name}")

            for record in records:
                record_json = json.dumps(record)
                record_size = len(record_json.encode('utf-8'))

                # Check if adding this record would exceed batch size
                if current_batch and (current_batch_size + record_size > MAX_BATCH_SIZE_BYTES):
                    # Send current batch
                    batch_counter += 1
                    
                    # Refresh DNS periodically to avoid cache issues
                    if batch_counter % DNS_CACHE_REFRESH_INTERVAL == 0:
                        logging.info(f"🔄 Periodic DNS refresh at batch #{batch_counter}")
                        self.refresh_all_dns()
                    
                    if send_batch_with_retry(current_batch, batch_counter, current_batch_size):
                        total_sent += len(current_batch)
                        progress_pct = (total_sent / total_records) * 100
                        elapsed = time.time() - start_time
                        rate = total_sent / elapsed if elapsed > 0 else 0
                        eta_seconds = (total_records - total_sent) / rate if rate > 0 else 0
                        
                        # Log every 25 batches or every 30 seconds (whichever comes first)
                        current_time = time.time()
                        should_log = (batch_counter % 25 == 0 or 
                                     batch_counter == 1 or 
                                     (current_time - last_progress_time) >= 30)
                        
                        if should_log:
                            logging.info(
                                f"✓ Sent batch #{batch_counter}: {len(current_batch)} records "
                                f"({current_batch_size / 1024:.1f} KB) for {self.env_name} "
                                f"[{total_sent:,}/{total_records:,} = {progress_pct:.1f}%] "
                                f"[Rate: {rate:.0f} rec/sec] [ETA: {eta_seconds / 60:.1f}m]"
                            )
                            last_progress_time = current_time
                    
                    # Reset batch (frees memory)
                    current_batch = []
                    current_batch_size = 0
                    
                    # Apply delay between batches
                    if BATCH_DELAY_MS > 0:
                        time.sleep(BATCH_DELAY_MS / 1000.0)

                # Add record to current batch
                current_batch.append(record)
                current_batch_size += record_size

                # Handle oversized single records
                if current_batch_size > MAX_BATCH_SIZE_BYTES and len(current_batch) == 1:
                    batch_counter += 1
                    logging.warning(
                        f"⚠️ Single record exceeds size limit ({current_batch_size / 1024:.1f} KB) "
                        f"for {self.env_name}, sending anyway as batch #{batch_counter}"
                    )
                    if send_batch_with_retry(current_batch, batch_counter, current_batch_size):
                        total_sent += 1
                        progress_pct = (total_sent / total_records) * 100
                        logging.info(
                            f"✓ Sent oversized batch #{batch_counter} for {self.env_name} "
                            f"[{total_sent:,}/{total_records:,} records = {progress_pct:.1f}%]"
                        )
                    
                    current_batch = []
                    current_batch_size = 0

            # Send remaining records
            if current_batch:
                batch_counter += 1
                if send_batch_with_retry(current_batch, batch_counter, current_batch_size):
                    total_sent += len(current_batch)
                    elapsed = time.time() - start_time
                    rate = total_sent / elapsed if elapsed > 0 else 0
                    
                    logging.info(
                        f"✓ Sent final batch #{batch_counter}: {len(current_batch)} records "
                        f"({current_batch_size / 1024:.1f} KB) for {self.env_name} "
                        f"[{total_sent:,}/{total_records:,} records = 100%] "
                        f"[Avg Rate: {rate:.0f} rec/sec]"
                    )

            elapsed = time.time() - start_time
            avg_rate = total_sent / elapsed if elapsed > 0 else 0
            logging.info(
                f"✅ Successfully completed: Sent {total_sent:,} total records "
                f"in {batch_counter} batch(es) to Log Analytics for {self.env_name} "
                f"in {elapsed / 60:.1f} minutes (avg {avg_rate:.0f} rec/sec)"
            )

        except Exception as e:
            elapsed = time.time() - start_time
            logging.error(
                f"❌ Failed to send data to Log Analytics for {self.env_name} "
                f"after processing {total_sent:,}/{total_records:,} records in {batch_counter} batches "
                f"over {elapsed / 60:.1f} minutes: {str(e)}"
            )
            raise

# ============================================================================
# MAIN PROCESSING FUNCTION
# ============================================================================

def process_salesforce_data():
    """Main processing function with deduplication"""
    try:
        # Pre-resolve Key Vault DNS
        kv_hostname = f"{KEY_VAULT_NAME}.vault.azure.net"
        dns_cache.resolve(kv_hostname)
        
        # Initialize Key Vault client
        kv_uri = f"https://{kv_hostname}"
        kv_client = SecretClient(vault_url=kv_uri, credential=credential)
        logging.info("Key Vault client initialized")

        # Initialize State Manager (pre-resolves Table Storage DNS internally)
        state_mgr = StateManager(STATE_STORAGE_ACCOUNT, credential)
        logging.info("State manager initialized")

        # Parse environments configuration
        environments = json.loads(ENVIRONMENTS_JSON)
        logging.info(f"Processing {len(environments)} environments")

        # Process each environment
        for env_config in environments:
            try:
                logging.info(f"\n{'='*70}")
                logging.info(f"Processing environment: {env_config['name']}")
                logging.info(f"{'='*70}")

                # Initialize processor for this environment (pre-resolves DNS internally)
                processor = SalesforceProcessor(env_config, credential, kv_client)

                # Authenticate with Salesforce
                processor.authenticate_salesforce()

                # Query for new log files
                log_files = processor.query_event_log_files(state_mgr)

                if not log_files:
                    logging.info(f"No new log files to process for {env_config['name']}")
                    continue

                logging.info(f"Processing {len(log_files)} new log files for {env_config['name']}")

                # Process each log file
                for log_file in log_files:
                    log_file_id = log_file.get('Id', '')
                    event_type = log_file.get('EventType', '')
                    log_date = log_file.get('LogDate', '')
                    sequence = log_file.get('Sequence', 0)
                    log_file_path = log_file.get('LogFile', '')

                    logging.info(f"Processing: {event_type}/{log_date}/Seq{sequence} (ID: {log_file_id})")

                    try:
                        # Download CSV
                        csv_content = processor.download_csv_file(log_file_path)

                        # Parse CSV to JSON
                        json_records = processor.parse_csv_to_json(csv_content, event_type, log_date, sequence)

                        if not json_records:
                            logging.warning(f"No records found in {event_type}/{log_date}/Seq{sequence}")
                            continue

                        # Send to Log Analytics
                        processor.send_to_log_analytics(json_records)

                        # Update state AFTER successful ingestion
                        state_mgr.update_last_processed(
                            processor.env_name,
                            event_type,
                            log_date,
                            sequence,
                            log_file_id
                        )

                        logging.info(f"✅ Successfully processed {event_type}/{log_date}/Seq{sequence}")

                    except Exception as e:
                        logging.error(f"❌ Failed to process log file {log_file_id}: {str(e)}")
                        import traceback
                        logging.error(f"Traceback: {traceback.format_exc()}")
                        # Continue with next log file
                        continue

                logging.info(f"✅ Completed processing for environment: {env_config['name']}")

            except Exception as e:
                logging.error(f"❌ Error processing environment {env_config['name']}: {str(e)}")
                import traceback
                logging.error(f"Traceback: {traceback.format_exc()}")
                # Continue with next environment
                continue

        logging.info("All environments processed")

    except Exception as e:
        logging.error(f"❌ Fatal error in main processing: {str(e)}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        raise

# ============================================================================
# AZURE FUNCTION ENTRY POINT
# ============================================================================

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.timer_trigger(schedule="0 0 * * * *",  # Every 60 minutes
                   arg_name="myTimer", 
                   run_on_startup=False,
                   use_monitor=False)
def SalesforceToSentinel(myTimer: func.TimerRequest) -> None:
    """
    Timer trigger function that runs every 60 minutes to fetch Salesforce event logs
    and send them to Azure Sentinel via Log Analytics.
    
    Schedule: Every 60 minutes (:00)
    
    Features:
    - Deduplication via state tracking
    - Recovery logic for missed runs
    - Comprehensive DNS caching for all endpoints (Salesforce, DCE, Key Vault, Table Storage)
    - Token caching to reduce overhead
    - Connection lifecycle management with 'Connection: close' header
    - Explicit session cleanup in finally blocks
    - **SERIALIZED REQUEST EXECUTION** - Threading lock prevents concurrent DNS lookups
    - Minimal connection pool (size=1) for maximum control
    - Configurable batch delay (400ms default)
    - Enhanced error handling and retry logic with timeout-specific handling
    - CSV download retry logic for DNS/network failures
    """
    if myTimer.past_due:
        logging.info('⚠️ The timer is past due!')

    logging.info('========== Salesforce to Sentinel function started ==========')

    try:
        process_salesforce_data()
        logging.info('========== Salesforce to Sentinel function completed successfully ==========')
            
    except Exception as e:
        logging.error(f'❌ ========== Error in Salesforce to Sentinel function: {str(e)} ==========')
        raise
