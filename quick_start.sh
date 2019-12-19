quick_start() {

    echo "### HELP SCRIPT RUN REPO COMMANDS ### "
    echo ""

    echo "run_sbt_test      : " "bash quick_start.sh  run_sbt_test"
    echo "run_sbt_compile   : " "bash quick_start.sh  run_sbt_compile"
    echo "run_sbt_run       : " "bash quick_start.sh  run_sbt_run"
    echo "run_sbt_test      : " "bash quick_start.sh  run_sbt_test"
    echo "run_sbt_assembly  : " "bash quick_start.sh  run_sbt_assembly"

    echo ""
    echo "### HELP SCRIPT RUN REPO COMMANDS ### "
}

run_sbt_test() {

    sbt test
}

run_sbt_compile() {

    sbt clean compile
}

run_sbt_run() {

    sbt run 
}

run_sbt_assembly() {

sbt clean compile && sbt assembly 

}

# run specific bash function via CLI
#https://superuser.com/questions/106272/how-to-call-bash-functions
quick_start
$1 $2 $3 $4 $5 
