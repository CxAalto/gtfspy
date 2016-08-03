import requests
import xml.etree.ElementTree as ET

req = requests.get('http://www.poikkeusinfo.fi/xml/v2/210120161220')
et = ET.fromstring(req.text.encode('utf-8'))
print ET.tostring(et, encoding='utf8', method='xml')

