import yaml


if __name__ == "__main__":
    with open('data.yaml', 'r') as f:
        try:
            d = yaml.load(f)
            print "parsing data.yaml succeeded"
            # print d
            # print yaml.dump(d)
        except Exception as e:
            print "parsing data.yaml did not succeed:\n"
            print e


