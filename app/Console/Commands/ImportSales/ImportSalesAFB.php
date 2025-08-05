<?php

namespace App\Console\Commands\ImportSales;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportSalesAfb extends Command
{
    protected $signature   = 'import:sales-afb';
    protected $description = 'AFB 売上 CSV を csv_afb へ取込む';

    /* ---------- ヘルパ ---------- */
    private static function toDate(?string $v): ?string
    {
        // 元ロジック移植
        return ($v !== null && $v !== '' && $v !== '0000-00-00 00:00:00')
            ? date('Y-m-d H:i:s', strtotime($v))
            : null;
    }
    private static function toInt(?string $v): int
    {
        if ($v === null || $v === '') return 0;
        return (int) floor((float) str_replace(',', '', $v));
    }

    /* ---------- 取込 ---------- */
    public function handle(): int
    {
        $sourceDir  = storage_path('app/csv/Sales');
        $csvPattern = "$sourceDir/*afb*.csv";
        $headers    = ['番号'];   // ヘッダー行の先頭セル

        foreach (glob($csvPattern) as $csv) {
            $tmp = tempnam(sys_get_temp_dir(), 'csv_') . '.csv';
            file_put_contents(
                $tmp,
                mb_convert_encoding(file_get_contents($csv), 'UTF-8', 'SJIS-win,CP932,UTF-8')
            );

            SimpleExcelReader::create($tmp)
                ->useDelimiter(',')
                ->noHeaderRow()
                ->getRows()
                ->reject(function ($row) use ($headers) {
                    $first = $row[0] ?? '';
                    return (
                        (is_array($row) && count(array_filter($row, fn($v) => $v !== '')) === 0) ||
                        in_array($first, $headers, true)
                    );
                })
                ->each(function (array $row) {
                    DB::table('csv_afb')->insert([
                        'click_time'  => self::toDate($row[1]  ?? null), // Click日
                        'occur_time'  => self::toDate($row[2]  ?? null), // 発生日
                        'fix_time'    => self::toDate($row[3]  ?? null), // 確定日
                        'pg_id'       => $row[4]   ?? null,             // PID
                        'pg_name'     => $row[5]   ?? null,             // プロモーション名
                        'site_id'     => $row[6]   ?? null,             // SID
                        'site_name'   => $row[7]   ?? null,             // サイト名
                        'site_url'    => $row[8]   ?? null,             // サイトURL
                        'reward'      => self::toInt($row[9]  ?? null), // 報酬（税抜）
                        'statement'   => $row[10]  ?? null,             // 状態
                        'pb_id'       => $row[11]  ?? null,             // ポイントバックID
                        'order_id'    => $row[12]  ?? null,             // 成果個別ID
                        'sales'       => self::toInt($row[13] ?? null), // 成果売上
                        'ad_id'       => $row[14]  ?? null,             // 原稿ID
                        'device'      => $row[15]  ?? null,             // デバイス
                        'referer'     => $row[16]  ?? null,             // リファラ
                        'keyword'     => $row[17]  ?? null,             // キーワード
                        'useragent'   => $row[18]  ?? null,             // User-Agent
                    ]);
                });

            unlink($tmp);
        }

        $this->info('AFB 売上データ取込完了');
        return Command::SUCCESS;
    }
}
