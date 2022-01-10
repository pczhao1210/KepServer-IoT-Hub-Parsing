import json

f = open('sample.json','r', encoding='utf8')
text=f.read()

rawjson = json.loads(text)

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

#print(values)
#print(type(values))