__author__ = 'subhash'

import csv
import os.path
import sys

pattern = '(?s)' \
          'product/productId:(.*?)' \
          'review/userId:(.*?)' \
          'review/profileName:(.*?)' \
          'review/helpfulness:(.*?)' \
          'review/score:(.*?)' \
          'review/time:(.*?)' \
          'review/summary:(.*?)' \
          'review/text:(.*?)'


def processRawData(filename):
    """
     Process raw movie reviews dataset to produce
        one dataset fo  r ratingscores for recommendation engine (pickup only the productId, userId, and Score)
        and another for review text - used for sentiment analysis (pickup only the review/sumary, review/text, and Score)
     As the file is large, we will read line by line and process to corresponding output files
    :param filename: inputfilename
    :return:
    """

    with open(filename) as fp, \
         open(os.path.splitext(filename)[0] + "-ratings.csv",'w') as wp1, \
         open(os.path.splitext(filename)[0] + "-reviews.csv",'w') as wp2:
        wp1_csv = csv.DictWriter(wp1,['product/productId', 'review/userId', 'review/score'], lineterminator='\n')
        wp1_csv.writeheader()
        wp2_csv = csv.DictWriter(wp2,['product/productId', 'review/userId', 'review/summary', 'review/text', 'review/score'], lineterminator='\n')
        wp2_csv.writeheader()

        review_dict = dict()
        i = 0
        for line in fp:
            pieces = line.split(':')
            if len(pieces) > 1:
                key = pieces[0]
                value = ':'.join(pieces[1:]).strip()
                if key == 'product/productId':
                    if review_dict:
                        # print ({k: review_dict[k] for k in ('product/productId', 'review/userId', 'review/score')})
                        wp1_csv.writerow({k: review_dict[k].replace("\n", " ").replace("\r", " ").replace("\t", " ") for k in ('product/productId', 'review/userId', 'review/score')})
                        wp2_csv.writerow({k: review_dict[k].replace("\n", " ").replace("\r", " ").replace("\t", " ") for k in ('product/productId', 'review/userId', 'review/summary', 'review/text', 'review/score')})
                        # wp1_csv.writerow({k: review_dict[k] for k in ('product/productId', 'review/userId', 'review/score')})
                        # wp2_csv.writerow({k: review_dict[k] for k in ('product/productId', 'review/userId', 'review/summary', 'review/text', 'review/score')})
                        review_dict.clear()
                        i = i+1
                        # if i == 20: break
                        if (i % 1000000 == 0):
                            print ("Processed file count:",i)
                review_dict[key] = value



def main(args):
    processRawData("/home/subhash/amazonreviews/movies.txt")
    # processRawData("/gluster/sbylaiah/amazonreviews/movies.txt")

if __name__ == '__main__':
    main(sys.argv[1:])