#!/usr/bin/env python

"""
Stand-alone script which reads a CSV file of data measurements (specified on
the command line) and generates a textual pdf, printed to standard out.
"""

from __future__ import division, print_function
from sys import argv,exit
import re

def read_csv_into_list(filename):
    """
    Read a csv file of floats, concatenate them all into a flat list and return
    them.
    """
    numbers = []
    for line in open(filename, 'r'):
        if not re.match('([0-9]+\.[0-9]+) ', line):
            for value in line.split(","):
                numbers.append(float(value))
    return numbers
    
def print_pdf(binsize, filename):
    """
    Read data values from file given by filename, and produces a pdf in text
    form. Each line in the printed output will contain two numbers, x y, where
    y is the number of items in the bin, and the range of the bin is (x_prev,
    x], where x_prev is x of the previous line. For the first line, x_prev is
    implicitly a number less than all the numbers in the dataset, meaning that
    y of the first line is the number if items in the dataset <= x.
    """
    # Read the file into an array of numbers.
    numbers = read_csv_into_list(filename)

    # Output to the current file + .cdf
    outfile = open(filename + ".pdf", 'w')

    # Generate a PDF from the array.
    numbers.sort() 
    i = 0
    count = 0
    bound = numbers[0] + binsize
    
    while True:
        if numbers[i] <= bound:
            count += 1
            i += 1

            if i == len(numbers):
                outfile.write("%8.4f    %d\n" % (bound, count))
                break
        else:
            outfile.write("%8.4f    %d\n" % (bound, count))
            count = 0
            bound += binsize
            
    outfile.close()

def usage():
    doc = """
    Usage: ./pdf.py <binsize> <input-file>

    Sample Input File:
    0.1210,0.1210,0.1200,0.1210,0.1200,0.1210,0.1200,0.1200,0.1200,0.1200
    0.1210,0.1200,0.1200,0.1210,0.1210,0.1200,0.1200,0.1200,0.1200,0.1200
    0.1200,0.1200,0.1200,0.1210,0.1210,0.1200,0.1251,0.1200,0.1200,0.1200
    0.1200,0.1200,0.1210,0.1200,0.1200,0.1238,0.1200,0.1200,0.1200,0.1210
    ...

    Sample Output:
    0.1100       1
    0.1200       3
    0.1300       78
    0.1400       123
    0.1500       675
    0.1600       1047
    0.1700       5345
    0.1800       3021
    0.1900       982
    0.2000       351
    ...
    3.0100       23
    3.0200       12
    3.0300       2

    """ 
    print(doc)
    exit(0)

if __name__ == '__main__': 
    if len(argv) < 3: 
       usage()
    print_pdf(float(argv[1]), argv[2])
