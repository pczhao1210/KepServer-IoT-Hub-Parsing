import asyncio
import json, datetime

from azure.eventhub.aio import EventHubConsumerClient

CONNECTION_STR = ["Your EventHub Connection String Here"]
EVENTHUB_NAME = ["Your EventHub Name Here"]


async def on_event(partition_context, event):
    # Put your code here.
    # If the operation is i/o intensive, async will have better performance.
    print(str(datetime.datetime.now()), "Received event from partition: {}.".format(partition_context.partition_id))
    #print(str(datetime.datetime.now()), "Received Data: {}.".format(event.message))
    rawjson = json.loads(str(event.message))
    #print(type(rawjson))
    values = rawjson['telemetry']['values']
    for data in values:
        kepware_tag = data['id']
        kepware_value = data['q']
        kepware_gen_time = data['t']
        kepware_refresh_success = data['q']

        data_template = '{{"kepware_tag":{kepware_tag},"kepware_value":{kepware_value},"kepware_gen_time":{kepware_gen_time},"kepware_refresh_success":{kepware_refresh_success}}}'

        kepware_data = data_template.format(
            kepware_tag=kepware_tag, 
            kepware_value=kepware_value, 
            kepware_gen_time=kepware_gen_time, 
            kepware_refresh_success=kepware_refresh_success
            )

        print(kepware_data)
        #Add your code here to write to SQL of do futhur processing
    await partition_context.update_checkpoint(event)



async def on_partition_initialize(partition_context):
    # Put your code here.
    print(str(datetime.datetime.now()), "Partition: {} has been initialized.".format(partition_context.partition_id))


async def on_partition_close(partition_context, reason):
    # Put your code here.
    print(str(datetime.datetime.now()), "Partition: {} has been closed, reason for closing: {}.".format(
        partition_context.partition_id,
        reason
    ))


async def on_error(partition_context, error):
    # Put your code here. partition_context can be None in the on_error callback.
    if partition_context:
        print(str(datetime.datetime.now()), "An exception: {} occurred during receiving from Partition: {}.".format(
            partition_context.partition_id,
            error
        ))
    else:
        print(str(datetime.datetime.now()), "An exception: {} occurred during the load balance process.".format(error))

async def main():
    client = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group="$default",
        eventhub_name=EVENTHUB_NAME
    )
    async with client:
        await client.receive(
            on_event=on_event,
            on_error=on_error,
            on_partition_close=on_partition_close,
            on_partition_initialize=on_partition_initialize,
            starting_position="-1",  # "-1" is from the beginning of the partition.
        )

if __name__ == '__main__':
    asyncio.run(main())