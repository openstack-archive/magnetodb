if is_service_enabled magnetodb; then

	if is_service_enabled magnetodb; then
	MAGNETODB_BACKEND=cassandra
	fi

    if [[ "$1" == "source" ]]; then
        # Initial source
        source $TOP_DIR/lib/magnetodb
    elif [[ "$1" == "stack" && "$2" == "install" ]]; then
        echo "Installing Magnetodb"
		install_${MAGNETODB_BACKEND}
        install_magnetodb
    elif [[ "$1" == "stack" && "$2" == "post-config" ]]; then
	echo "Configuring Magneto"

		configure_${MAGNETODB_BACKEND}

	configure_magnetodb        


:
    elif [[ "$1" == "stack" && "$2" == "extra" ]]; then
        # no-op

		start_${MAGNETODB_BACKEND}
		#waiting for backend start
        start_magnetodb


    fi

    if [[ "$1" == "unstack" ]]; then
	echo  "Stopping Magnetodb"

		stop_${MAGNETODB_BACKEND}
        stop_magnetodb
    fi

    if [[ "$1" == "clean" ]]; then
        :
		clean_${MAGNETODB_BACKEND}
    fi
fi

