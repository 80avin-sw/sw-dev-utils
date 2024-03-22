# sample usage:
# ./bench-all.sh
# ./bench-all.sh ./std-progress
# ./bench-all.sh ./std-progress 6
echo $@
times=6
analysis_dir="/Users/avinashthakur/edu-api/analysis/std-progress"

script_dir="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

function do_bench() {
	analysis_dir="${1:-/Users/avinashthakur/edu-api/analysis/std-progress}"
	output_dir="$([[ -d $analysis_dir ]] && echo $analysis_dir || dirname "$(realpath $analysis_dir)")"
	times="${2:-6}"

	output_filename=$(date "+%Y-%m-%d_%H-%M-%S")
	output_file="$output_dir/$output_filename.txt"
	touch $output_file
	node $script_dir/bench-all.js $times $analysis_dir | tee -a $output_file
}

while [[ $# -ne "" ]]; do
	key="$1"
	if [[ "$1" -gt 0 ]] 2>/dev/null; then
		times=$1
	elif [[ -e "$1" ]]; then
		do_bench $1 $times
	fi
	shift
done

exit
