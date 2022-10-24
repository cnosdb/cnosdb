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
		cargo build
                """
            }
        }

	    stage('test') {
            steps {

                sh """
		cd $WORKSPACE
		cargo test --workspace --all-features --exclude e2e_test
                """
            }
	    }
	}
}