# Topaz
*Splunk CLI*

![alt text](./topaz.jpg)

## Installation
```
git clone ssh://git@github.com/MartinCastroAlvarez/topaz
cd topaz
virtualenv -p python3 env
source env/bin/activate
pip install -r requirements.txt
```

## Configuration
Put the following content in *$HOME/.topaz*
```
{
    "my_company": {
        "account": "my_company",
        "username": "###########",
        "password": "####"
    }
}
```


## Usage

#### Search Splunk alerts in the last 15 minutes.
```
python3 topaz.py --index sandbox --search seagull --start 15m
```
```
901:80''>
<Alert: '    2019/04/12 15:52:41 [INFO] agent: Synced service 'ip-10-0-12-123:ecs-pmts5-seagull-31-pmts5-seagull-ba9cdff4c0939b819901:80''>
<Alert: '    2019/04/12 15:52:11 [INFO] agent: Synced service 'ip-10-0-12-123:ecs-pmts5-seagull-31-pmts5-seagull-ba9cdff4c0939b819901:80''>
<Alert: '    2019/04/12 15:51:41 [INFO] agent: Synced service 'ip-10-0-12-123:ecs-pmts5-seagull-31-pmts5-seagull-ba9cdff4c0939b819901:80''>
<Alert: '> Start monitoring Consul & Python...'>
<Alert: '> Starting python app: seagull...'>
<Alert: '> Start consul-template in daemon mode...'>
<Alert: '> Running consul-template once...'>
<Alert: '> CONSUL_IP is 10.0.12.123'>
<Alert: '> CONSUL_IP not set, looking for AWS instance IP'>
<Alert: '> Checking for CONSUL_IP environment variable'>
<Alert: '> Starting start.sh of base-pyservice...'>
<Alert: '2019/04/12 15:51:23 added: 73bbf17361ce ip-10-0-12-123:ecs-pmts5-seagull-31-pmts5-seagull-ba9cdff4c0939b819901:80'>
<Alert: '    2019/04/12 15:51:23 [INFO] agent: Synced service 'ip-10-0-12-123:ecs-pmts5-seagull-31-pmts5-seagull-ba9cdff4c0939b819901:80''>
```
### Search for startup errors.
```
python3 topaz.py --index sandbox --search startup --start 15m
```
