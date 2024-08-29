
for YEAR_START in $(seq 1960 8 2024); do

    YEAR_END=$(($YEAR_START+7))
    export YEAR_START
    export YEAR_END
    export DOCKER_TAG=$YEAR_START-$YEAR_END
    make build
    make push
    make runai-submit
    break
done
