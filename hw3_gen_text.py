
import argparse
import os
import sys
import re
import multiprocessing
import random
from collections import Counter


def get_stat_for_file(filepath):
    print >> sys.stderr, filepath
    sys.stderr.flush()

    text_bytes = open(filepath, 'rb').read()
    text_u = text_bytes.decode('UTF-8')

    ngrams_stat = Counter()

    apostrophe = unichr(8217)
    text_u = text_u.replace(apostrophe, "'")
    sentences = re.findall(r'[^.?!\n]+', text_u)

    # save comma as part of word, to save proper punctuation
    # save quote ' and hyphen - as part of word, when surrounded by letters
    word_re = re.compile(ur"(?:(?:\w-\w)|(?:\w'\w)|\w)+,?", re.UNICODE)
    for sentence in sentences:
        words = word_re.findall(sentence)
        if not words:
            continue
        for w in words:
            # space used as separator in saved files
            assert ' ' not in w
            # '$' used as start/end sentence sign in saved files
            assert '$' not in w
        # fake words to simplify statistics
        words = ['$start1', '$start2'] + words + ['$end1', '$end2']
        n_gram_len = 3
        for i in xrange(len(words) - n_gram_len + 1):
            gram = ' '.join(words[i:i + n_gram_len]).encode('UTF-8')
            ngrams_stat[gram] += 1

    return ngrams_stat


def preprocess(input_dir, output_file):
    assert os.path.isdir(input_dir), 'invalid directory %s' % input_dir

    input_files = []
    for path, _dirs, files in os.walk(input_dir):
        for f in files:
            filepath = os.path.join(path, f)
            input_files.append(filepath)

    p = multiprocessing.Pool(6)
    results = list(p.map(get_stat_for_file, input_files))
    print >> sys.stderr, 'done reading files.'
    stat = Counter()
    for ngrams_stat in results:
        for ngram, count in ngrams_stat.iteritems():
            stat[ngram] += count

    print >> sys.stderr, 'done summation'
    with open(output_file, 'wb') as out:
        for ngram, count in stat.items():
            print >> out, '%s\t%s' % (ngram, count)


def load_stat_from_file(input_stat_file):
    stat = {}
    for line in open(input_stat_file, 'rb'):
        ngram, count = line.rstrip('\r\n').split('\t')
        count = int(count)
        i_last_space = ngram.rfind(' ')
        assert i_last_space > 0
        ngram_prefix = ngram[:i_last_space]
        ngram_last_word = ngram[i_last_space+1:]
        # Zipf law ensure memory overhead is less than 2, not thousands.
        stat.setdefault(ngram_prefix, []).extend([ngram_last_word]*count)
    return stat


def generate_text(input_stat_file, word_limit):
    stat = load_stat_from_file(input_stat_file)
    word_count = 0
    while word_count < word_limit:
        words = ['$start1', '$start2']
        while words[-1] != '$end2':
            ngram_prefix = words[-2] + ' ' + words[-1]
            ngram_last_word = random.choice(stat[ngram_prefix])
            words.append(ngram_last_word)
            word_count += 1
        print ' '.join(words[2:-2]) + '.'
        print


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--preprocess-input-dir', dest='input_dir')
    parser.add_argument('--output-file', dest='output_file')
    parser.add_argument('--generate-text-from-stat', dest='input_stat_file')
    parser.add_argument(
        '--generate-text-word-limit', dest='word_limit', type=int)
    args = parser.parse_args()

    something_is_done = False
    if args.input_dir:
        assert args.output_file
        preprocess(args.input_dir, args.output_file)
        something_is_done = True

    if args.input_stat_file:
        assert args.word_limit is not None
        generate_text(args.input_stat_file, args.word_limit)
        something_is_done = True

    if not something_is_done:
        print >> sys.stderr, 'usage:'
        print >> sys.stderr, (
            '%s --preprocess-input-dir <dir> --output-file <file>' %
            sys.argv[0])
        print >> sys.stderr, 'or'
        parser.print_help()
        exit(1)

if __name__ == "__main__":
    main()
