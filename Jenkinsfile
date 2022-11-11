readTrusted 'Dockerfile_jenkins'

pipeline {
	agent {
        dockerfile {
          filename 'Dockerfile_jenkins'
	    }
    }

    environment {
        WS = "WORKSPACE"
    }

    stages {
        stage('Checking environment') {
            steps {
               echo "Detecting basic information..."
               sh 'cargo -V'
               sh 'rustc -V'
            }
        }

        stage('build') {
            steps {
                sh """
		cd $WORKSPACE
		cargo build --release --bin main
                """
            }
        }

        stage('clippy check') {
            steps {

                sh """
        	cd $WORKSPACE
        	cargo clippy --workspace --all-features --all-targets -- -D warnings
                   """
            }
        }


	    stage('unit-test') {
            steps {

                sh """
		cd $WORKSPACE
		cargo test --workspace --all-features --exclude e2e_test
                """
            }
	    }


	    stage('integration test') {
            steps {
                sh """
        	cd $WORKSPACE
        	bash ./query_server/test/script/start_and_test.sh
                  """
            }
        }

	}
}