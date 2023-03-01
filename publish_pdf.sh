#!/bin/bash

if [ -z $1 ]; then
        echo "usage: ./publish_pdf.sh [exp_number e.g., 14]"
        exit 0
fi

pushd notebooks
jupyter nbconvert --to latex --no-input  -TagRemovePreprocessor.remove_cell_tags='{"verbose"}' $1.ipynb --output $1_summary
jupyter nbconvert $1.ipynb --to latex --no-input --output $1_verbose
pdflatex $1_summary.tex
pdflatex $1_verbose.tex
rm -rf output/$1*
mv $1* output
mv output/$1.ipynb .
cp output/$1_summary_files/* /valinordatasets/pngs
cp output/$1_verbose_files/* /valinordatasets/pngs
cp output/$1_summary.pdf /valinordatasets/pdfs
cp output/$1_verbose.pdf /valinordatasets/pdfs
popd
echo "Successfully published PDFs for $1"
