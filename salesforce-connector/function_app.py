import azure.functions as func
import logging
import os
import json
import csv
import io
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Import external packages at MODULE LEVEL
import requests
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.data.tables import TableClient

# Configuration from environment variables
KEY_VAULT_NAME = os.environ.get('KEY_VAULT_NAME')
DCE_ENDPOINT = os.environ.get('DCE_ENDPOINT')
STREAM_NAME = os.environ.get('STREAM_NAME')
ENVIRONMENTS_JSON = os.environ.get('ENVIRONMENTS_JSON')
STATE_STORAGE_ACCOUNT = os.environ.get('STATE_STORAGE_ACCOUNT')


# ============================================================================
# STATE MANAGER CLASS
# ============================================================================

class StateManager:
    """Manages processing state to prevent duplicates and recover missed logs"""

    def __init__(self, storage_account_name: str, credential):
        self.table_client = TableClient(
            endpoint=f"https://{storage_account_name}.table.core.windows.net",
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

            response = requests.post(token_url, data=data)
            response.raise_for_status()
            self.access_token = response.json()['access_token']
            logging.info(f"Successfully authenticated with Salesforce for {self.env_name}")
            return self.access_token
        except Exception as e:
            logging.error(f"Salesforce authentication failed for {self.env_name}: {str(e)}")
            raise

    def query_event_log_files(self, state_mgr: StateManager) -> List[Dict]:
        """Query Salesforce for NEW event log files only (deduplication applied)"""
        try:
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
            response = requests.get(query_url, headers=headers, params={'q': query})
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
        """Download CSV log file from Salesforce"""
        try:
            headers = {'Authorization': f'Bearer {self.access_token}'}
            url = f"{self.sf_domain}{log_file_path}"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logging.error(f"Failed to download CSV file {log_file_path}: {str(e)}")
            raise

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
                    'UniqueLogFileId': unique_id,  # NEW: For query-time deduplication
                    'LogFileSequence': sequence,    # NEW: Track sequence
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
        """Send parsed records to Log Analytics via DCE"""
        try:
            if not records:
                logging.info(f"No records to send for {self.env_name}")
                return

            # Get Azure Monitor access token
            token = self.credential.get_token("https://monitor.azure.com/.default")

            # Prepare the DCE URL
            dce_url = f"{DCE_ENDPOINT}/dataCollectionRules/{self.dcr_immutable_id}/streams/{STREAM_NAME}?api-version=2023-01-01"

            headers = {
                'Authorization': f'Bearer {token.token}',
                'Content-Type': 'application/json'
            }

            # Send in batches of 500 records
            batch_size = 500
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                response = requests.post(dce_url, headers=headers, json=batch)
                response.raise_for_status()
                logging.info(f"Sent {len(batch)} records to Log Analytics for {self.env_name}")
        except Exception as e:
            logging.error(f"Failed to send data to Log Analytics for {self.env_name}: {str(e)}")
            raise


# ============================================================================
# MAIN PROCESSING FUNCTION
# ============================================================================

def process_salesforce_data():
    """Main processing function with deduplication"""
    try:
        # Initialize Azure credential
        credential = DefaultAzureCredential()

        # Initialize Key Vault client
        kv_uri = f"https://{KEY_VAULT_NAME}.vault.azure.net"
        kv_client = SecretClient(vault_url=kv_uri, credential=credential)

        # Initialize State Manager
        state_mgr = StateManager(STATE_STORAGE_ACCOUNT, credential)
        logging.info("State manager initialized")

        # Parse environments configuration
        environments = json.loads(ENVIRONMENTS_JSON)
        logging.info(f"Processing {len(environments)} environments")

        # Process each environment
        for env_config in environments:
            try:
                logging.info(f"Processing environment: {env_config['name']}")
                processor = SalesforceProcessor(env_config, credential, kv_client)

                # Authenticate with Salesforce
                processor.authenticate_salesforce()

                # Query for NEW event log files (deduplication applied)
                log_files = processor.query_event_log_files(state_mgr)

                if len(log_files) == 0:
                    logging.info(f"No new log files to process for {env_config['name']}")
                    continue

                # Process each log file
                for log_file in log_files:
                    event_type = log_file.get('EventType', 'Unknown')
                    log_file_path = log_file.get('LogFile', '')
                    log_date = log_file.get('LogDate', '')
                    sequence = log_file.get('Sequence', 0)
                    log_file_id = log_file.get('Id', '')

                    if not log_file_path:
                        continue

                    logging.info(f"Processing: {event_type} | {log_date} | Seq={sequence}")

                    # Download CSV
                    csv_content = processor.download_csv_file(log_file_path)

                    # Parse CSV to JSON
                    json_records = processor.parse_csv_to_json(
                        csv_content, event_type, log_date, sequence
                    )

                    # Send to Log Analytics
                    processor.send_to_log_analytics(json_records)

                    # Update state AFTER successful ingestion
                    state_mgr.update_last_processed(
                        processor.env_name, event_type, log_date, sequence, log_file_id
                    )

                logging.info(f"Completed processing for environment: {env_config['name']}")
            except Exception as e:
                logging.error(f"Error processing environment {env_config['name']}: {str(e)}")
                continue

        logging.info("All environments processed")
    except Exception as e:
        logging.error(f"Fatal error in main processing: {str(e)}")
        raise


# ============================================================================
# AZURE FUNCTION DEFINITION
# ============================================================================

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.timer_trigger(schedule="0 0 * * * *", 
                   arg_name="myTimer", 
                   run_on_startup=False,
                   use_monitor=False)
def SalesforceToSentinel(myTimer: func.TimerRequest) -> None:
    """
    Timer trigger function that runs every hour to fetch Salesforce event logs
    and send them to Azure Sentinel via Log Analytics.
    Includes deduplication and recovery logic.
    """
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Salesforce to Sentinel function started')

    try:
        process_salesforce_data()
        logging.info('Salesforce to Sentinel function completed successfully')
    except Exception as e:
        logging.error(f'Error in Salesforce to Sentinel function: {str(e)}')
        raise
