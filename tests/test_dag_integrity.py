import unittest
from airflow.models import DagBag

class TestDagIntegrity(unittest.TestCase):
    def test_dagbag_import_errors(self):
        """DAG 파일들에 문법 에러나 임포트 에러가 없는지 검사합니다."""
        # dags 폴더 위치를 지정합니다.
        dagbag = DagBag(dag_folder='dags', include_examples=False)
        
        # 에러 메시지가 있다면 출력하고 테스트를 실패시킵니다.
        error_msg = f"DAG 임포트 에러 발생: {dagbag.import_errors}"
        self.assertFalse(len(dagbag.import_errors) > 0, error_msg)

if __name__ == "__main__":
    unittest.main()
