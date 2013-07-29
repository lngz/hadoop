

LATIN_PRG=""
FIRSTLINE=0
while read DOMAIN
do
if [ $FIRSTLINE == 0 ] 
then
    LATIN_PRG=$DOMAIN
    FIRSTLINE=1
else
    LATIN_PRG="$LATIN_PRG OR $DOMAIN"
fi

done < domain_list

echo $LATIN_PRG
