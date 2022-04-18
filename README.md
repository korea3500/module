# module
custom module set 



### read_vcf 
* VCF 파일을 읽은 후 text format으로 변환합니다. 이 때, 사용하는 엔진을 pandas 또는 pyspark 중 선택하여 사용할 수 있습니다.
* 처리 process 는 다음과 같습니다.
* 먼저, meta information 을 저장하는 부분을 제거합니다.
* 이후, 데이터에 해당하는 부분만 추출하여 tsv파일 형태로 저장합니다.
* TO-DO 데이터에 따른 예외처리
