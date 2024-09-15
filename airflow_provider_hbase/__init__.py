def get_provider_info():
    return {
        # PyPI에 등록될 패키지 이름. Airflow 생태계에서 이 프로바이더를 식별하는 데 사용됨
        "package-name": "airflow-provider-hbase",
        "name": "Hbase", # Airflow UI나 로그에서 이 프로바이더를 지칭할 때 사용됨
        "description": "apache airflow custom hbase provider",
        
        # 이 프로바이더가 제공하는 연결 유형에 대한 정보
        "connection-types": [
            {
                # Airflow에서 이 연결을 식별하는 데 사용되는 고유 식별자
                "connection-type": "hbase",
                # 이 연결 유형에 대한 Hook 클래스의 전체 경로
                "hook-class-name": "airflow_provider_hbase.hooks.hbase.HBaseHook"
            }
        ],
    }