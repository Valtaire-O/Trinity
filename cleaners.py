import re

class TextWorks:
    '''Basic text pattern matching and cleaning'''
    def text_patterns(self, feature_list, value):
        '''dynamic regex matching'''
        pattern = '|'.join([i for i in feature_list])
        match = re.compile(rf'{pattern}', flags=re.IGNORECASE).search(value)
        return match

    def clean_text(self, text,link=False):
        out = []
        if link:
            clean = re.sub('[^A-Za-z0-9]+', ' ', text).split()
            return ' '.join(clean)
        else:
            clean = re.sub('\W+', ' ', text).split()

        for c in clean:
            if c:
                # keep only alpabetical chars in word
                really_clean = "".join(filter(lambda x: x.isalpha(), c))
                out.append(really_clean)

        # cleaned_text = ' '.join([i for i in cleaned_text if i.strip()])
        return [i for i in out if i]

    def negative_match(self, text):
        bad_text = ['contact us', 'sign', 'login', 'account', 'subscribe', 'javascript', 'about us',
                    'privacy', 'agreement', 'sales', '#', 'help', 'support', 'info',
                    'faqs', 'none', 'author', 'page', 'mailto', 'user', 'admin', 'tel:', 'submission', 'format='

                    ]
        return self.text_patterns(bad_text, text)
    def negative_match_domain(self, domain, base_url):

        bad_domains = ['linkedin', 'instagram', 'facebook', 'twitter', 'snapchat', 'tiktok', 'google']
        check_domain = self.text_patterns(bad_domains, base_url)
        if check_domain:
            return None

        return self.text_patterns(bad_domains, domain)
