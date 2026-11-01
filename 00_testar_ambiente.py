import sys, platform
from pathlib import Path
print('=== PRÉ-FLIGHT · Semana 11 ===')
print('Python:', sys.version.split()[0])
print('Sistema:', platform.system(), platform.release())
for pasta in ['dados','lake/raw','lake/silver','lake/gold','reports','governance']:
    Path(pasta).mkdir(parents=True, exist_ok=True)
    print('✓ pasta ok:', pasta)
print('\nAMBIENTE OK — abra este print no caderno como evidência.')
