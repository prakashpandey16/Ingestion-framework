import datetime
import logging
from sqlalchemy.orm import sessionmaker
from conf.config import engine,batch_config_table,batch_audit_log_table,batch_run_log_table
from decorators.decorators import log_execution_time, handle_errors


logger = logging.getLogger(__name__)


Session = sessionmaker(bind=engine)
session = Session()

@handle_errors
@log_execution_time
def schedule_batches():
    now = datetime.datetime.now()
    current_time = now.time()
    try:
        # Fetch all batch configs
        print(f"Came here to get all config tables audit logs {current_time} ++{session.info}")
        batch_configs = session.execute(
            batch_config_table.select()
        ).fetchall()
        #batch_configs=session.query(BatchConfig).all()
        print(f"Got the config tables {batch_configs}")

        eligible_batches = []
        for config in batch_configs:
            print(f"Found a config")
            last_run = session.execute(
                batch_run_log_table.select()
                .where(batch_run_log_table.c.batch_id == config.batch_id)
                .order_by(batch_run_log_table.c.run_start.desc())
                .limit(1)
            ).fetchone()

            # Calculate the next run time based on frequency
            frequency_to_timedelta = {
                'high': datetime.timedelta(minutes=5),
                'medium': datetime.timedelta(hours=1),
                'low': datetime.timedelta(days=1)
            }
            if last_run:
                next_run_time = last_run.run_start + frequency_to_timedelta[config.frequency] if last_run else datetime.datetime.min
                print(f"Last run time :{last_run.run_start}Next run time {next_run_time} Now{now} Current Time {current_time} Configstarttime {config.start_time} ")
            # Check if it's time to run the batch
                if next_run_time <= now : #and current_time >= config.start_time:this can be used if we want the start to happen only after particular time of the day    
            
                    print(f"Came to run the batch")
                    eligible_batches.append(config)
            else:
                eligible_batches.append(config) # this is the first run of the batch hence
                
    
        return eligible_batches

    except Exception as e:
        logger.error(f"Error scheduling batches: {e}")
        raise

    finally:
        session.close()
