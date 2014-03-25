if is_service_enabled magnetodb; then
    if [[ "$1" == "source" ]]; then
        # Initial source
        source $TOP_DIR/lib/magnetodb
    elif [[ "$1" == "stack" && "$2" == "install" ]]; then
        echo "Installing Magnetodb"
        install_magnetodb
    elif [[ "$1" == "stack" && "$2" == "post-config" ]]; then
	echo "Configuring Magneto"
	configure_magnetodb        
:
    elif [[ "$1" == "stack" && "$2" == "extra" ]]; then
        # no-op
        start_magnetodb
    fi

    if [[ "$1" == "unstack" ]]; then
	echo  "Stopping Magnetodb"
        stop_magnetodb
    fi

    if [[ "$1" == "clean" ]]; then
        clean_magnetodb
    fi
fi
