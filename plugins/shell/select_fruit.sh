FRUIT=$1
if [ $FRUIT == APPLE ]; then
	echo "select apple"
elif [ $FRUIT == ORANGE ]; then
	echo "select orange"
elif [ $FRUIT == GRAPE]; then
	echo "select grape"
else 
	echo " other fruit!"
fi
